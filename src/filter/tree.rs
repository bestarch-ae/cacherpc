use std::collections::hash_map::{Entry, IntoIter as HashMapIntoIter, Iter as HashMapIter};
use std::collections::HashMap;

use smallvec::SmallVec;
use tracing::error;

use super::{Filter, Filters, Memcmp, Pattern, Range, RESERVED_RANGE};
use crate::types::AccountData;

use node::MemcmpNode;

type Registered<T> = Option<T>;

/// Collects all matching filters into Vec
#[cfg(test)]
pub fn collect_all_matches<T>(tree: &FilterTree<T>, data: &AccountData) -> Vec<Filters> {
    let mut vec = Vec::new();
    let closure = |filters| vec.push(filters);

    tree.map_matches(data, closure);
    vec
}

macro_rules! assert_or_err {
    ($cond:expr $(, $($arg:tt)+ )?) => {
        debug_assert!($cond $(, $($arg)+ )?);
        $(if !$cond { error!($($arg)+); })?
    };
}

#[derive(Default)]
#[cfg_attr(test, derive(Clone))] // Should this be cloneable?
pub struct FilterTree<T> {
    // None is for filter groups that do not contain datasize filter
    data_size: HashMap<Option<u64>, MemcmpNode<T>>,
    len: usize,
}

impl<T> FilterTree<T> {
    pub fn new() -> Self {
        Self {
            data_size: HashMap::new(),
            len: 0,
        }
    }

    pub fn insert(&mut self, filters: Filters, value: T) -> Option<T> {
        let mut node = self.data_size.entry(filters.data_size).or_default();

        let memcmp = filters.memcmp;

        for filter in memcmp {
            let range = filter.range();
            node = node
                .children
                .entry(range)
                .or_default()
                .entry(filter.bytes)
                .or_default();
        }

        if node.registered.is_none() {
            self.len = self.len.saturating_add(1);
        }

        node.registered.replace(value)
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn remove(&mut self, filters: &Filters) -> Option<T> {
        let mut node = self.data_size.get_mut(&filters.data_size)?;

        let memcmp = &filters.memcmp;
        // Find the highest dead node
        let mut remove_from = None;

        for (idx, filter) in memcmp.into_iter().enumerate() {
            let range = filter.range();
            let mut to_remove = node.registered.is_none() && node.children.len() < 2;
            let by_pattern = node.children.get_mut(&range)?;
            // Schedule a node for removal if it has no value and not more than one child
            // (that we're indexing into).
            // Check the previous node so the zero index would correpond to data_size layer
            // and applying idx filters (including data_size) to the tree
            // would yield the node before the one to remove from and the filter to remove at.
            to_remove &= by_pattern.len() < 2;

            node = by_pattern.get_mut(&filter.bytes)?;

            if to_remove {
                remove_from.get_or_insert(idx);
            } else {
                // A not-ok-to-remove node invalidates it's parents
                remove_from = None;
            }
        }

        let value = node.registered.take();
        if !node.children.is_empty() {
            // If this is not a leaf node we cannot remove anything we scheduled
            remove_from.take();
        }

        if value.is_some() {
            self.len = self.len.saturating_sub(1);
            match remove_from {
                Some(0) => {
                    self.data_size.remove(&filters.data_size);
                }
                Some(idx) => {
                    let idx = idx - 1; // idx > 0
                    let mut maybe_node = self.data_size.get_mut(&filters.data_size);

                    for filter in memcmp.into_iter().take(idx) {
                        if let Some(node) = maybe_node.take() {
                            maybe_node = node
                                .children
                                .get_mut(&filter.range())
                                .and_then(|by_pattern| by_pattern.get_mut(&filter.bytes));
                        } else {
                            break;
                        }
                    }

                    let maybe_node = maybe_node.zip(memcmp.get(idx)).and_then(|(node, filter)| {
                        match node.children.entry(filter.range()) {
                            Entry::Occupied(entry) => Some((entry, filter)),
                            Entry::Vacant(_) => None,
                        }
                    });

                    if let Some((mut node, filter)) = maybe_node {
                        let removed = node.get_mut().remove(&filter.bytes);
                        let removed_ok = removed.map_or(true, |node| {
                            node.registered.is_none() && node.children.len() <= 1
                        });
                        assert_or_err!(removed_ok, "removed node that was not supposed to");
                        if node.get_mut().is_empty() {
                            node.remove();
                        }
                    } else {
                        assert_or_err!(false, "could not index into node for deletion");
                    }
                }
                None => (),
            }
        }

        value
    }

    pub fn map_matches(&self, data: &AccountData, f: impl FnMut(Filters)) {
        let data_len = data.len() as u64;

        let iter = std::iter::once(self.data_size.get_key_value(&Some(data_len)))
            .chain(std::iter::once(self.data_size.get_key_value(&None)))
            .flatten();

        let iter = IterMatches {
            account_data: data,
            roots_iter: iter,
            node_stack: SmallVec::new(),
        };

        iter.map(|(k, _v)| k).for_each(f);
    }
}

impl<T> Drop for FilterTree<T> {
    fn drop(&mut self) {
        let data_size = std::mem::take(&mut self.data_size);
        for (_, node) in data_size.into_iter() {
            drop(node);
        }
    }
}

impl<T> IntoIterator for FilterTree<T> {
    type Item = (Filters, T);
    type IntoIter = IntoIter<T>;

    fn into_iter(mut self) -> Self::IntoIter {
        let data_size = std::mem::take(&mut self.data_size);
        IntoIter {
            roots_iter: data_size.into_iter(),
            node_stack: Vec::new(),
        }
    }
}

#[derive(Clone, Copy)]
enum FilterRef<'a> {
    DataSize(u64),
    Memcmp { offset: usize, bytes: &'a [u8] },
}

impl FilterRef<'_> {
    fn into_owned(self) -> Filter {
        match self {
            FilterRef::DataSize(size) => Filter::DataSize(size),
            FilterRef::Memcmp { offset, bytes } => Filter::Memcmp(Memcmp {
                offset,
                bytes: Pattern::from_slice(bytes),
            }),
        }
    }
}

type PatternMap<T> = HashMap<Pattern, MemcmpNode<T>>;
// Keep the iterator over registered ranges and the corresponding filter prefix for each layer
type StackItem<'a, T> = (Option<FilterRef<'a>>, HashMapIter<'a, Range, PatternMap<T>>);

pub struct IterMatches<'a, I, T> {
    account_data: &'a AccountData,
    roots_iter: I,
    node_stack: SmallVec<[StackItem<'a, T>; 4]>,
}

impl<I, T> IterMatches<'_, I, T> {
    fn collect_current_filter(&self) -> Filters {
        let filters = self
            .node_stack
            .iter()
            .filter_map(|(filter, _)| *filter)
            .map(FilterRef::into_owned);
        Filters::new_normalized(filters).expect("encountered bad filter group")
    }
}

impl<'a, I, T> Iterator for IterMatches<'a, I, T>
where
    I: Iterator<Item = (&'a Option<u64>, &'a MemcmpNode<T>)>,
{
    type Item = (Filters, &'a T);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some((_current_filter, current_node)) = self.node_stack.last_mut() {
                match current_node.next() {
                    Some((range, new_nodes)) => {
                        let next_node = self
                            .account_data
                            .data
                            .get(range.0..range.1)
                            .and_then(|slice| new_nodes.get_key_value(slice));

                        if let Some((pattern, node)) = next_node {
                            let is_leaf = node.registered.as_ref();
                            let offset = range.0;
                            let bytes = pattern;
                            let new_memcmp = FilterRef::Memcmp { offset, bytes };

                            self.node_stack
                                .push((Some(new_memcmp), node.children.iter()));

                            if let Some(value) = is_leaf {
                                return Some((self.collect_current_filter(), value));
                            }
                        }
                    }
                    None => {
                        self.node_stack.pop();
                    }
                }
            } else {
                let (data_size, node) = self.roots_iter.next()?;
                // Though Filters implementation guarantees empty group is invalid.
                let is_leaf = data_size.and(node.registered.as_ref());
                self.node_stack
                    .push((data_size.map(FilterRef::DataSize), node.children.iter()));
                if let Some(value) = is_leaf {
                    return Some((self.collect_current_filter(), value));
                }
            }
        }
    }
}

pub struct IntoIter<T> {
    roots_iter: HashMapIntoIter<Option<u64>, MemcmpNode<T>>,
    node_stack: Vec<(Option<Filter>, node::IntoIter<T>)>,
}

impl<T> IntoIter<T> {
    fn collect_current_filter(&self) -> Filters {
        let filters = self
            .node_stack
            .iter()
            .filter_map(|(filter, _)| filter.as_ref())
            .cloned();
        Filters::new_normalized(filters).unwrap()
    }
}

impl<T> Iterator for IntoIter<T> {
    type Item = (Filters, T);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some((_current_filter, current_node)) = self.node_stack.last_mut() {
                match current_node.next() {
                    Some((new_memcmp, mut new_node)) => {
                        let is_leaf = new_node.registered.take();
                        self.node_stack
                            .push((Some(Filter::Memcmp(new_memcmp)), new_node.into_iter()));
                        if let Some(value) = is_leaf {
                            return Some((self.collect_current_filter(), value));
                        }
                    }
                    None => {
                        self.node_stack.pop();
                    }
                }
            } else {
                let (data_size, mut node) = self.roots_iter.next()?;
                // Though Filters implementation guarantees empty group is invalid.
                let is_leaf = data_size.and(node.registered.take());
                self.node_stack
                    .push((data_size.map(Filter::DataSize), node.into_iter()));
                if let Some(value) = is_leaf {
                    return Some((self.collect_current_filter(), value));
                }
            }
        }
    }
}

mod node {
    use super::*;

    #[cfg_attr(test, derive(Clone, Debug))]
    pub struct MemcmpNode<T> {
        pub(super) registered: Registered<T>,
        pub(super) children: HashMap<Range, HashMap<Pattern, MemcmpNode<T>>>,
    }

    const PARENT_RANGE: Range = RESERVED_RANGE;

    // These are helper methods for Drop implementation
    impl<T> MemcmpNode<T> {
        fn take_parent(&mut self) -> Option<Self> {
            self.children.remove(&PARENT_RANGE)?.remove([].as_ref())
        }

        fn set_parent(&mut self, parent: Self) -> Option<Self> {
            self.children
                .entry(PARENT_RANGE)
                .or_default()
                .insert(SmallVec::new(), parent)
        }

        fn take_next_child_node(&mut self) -> Option<MemcmpNode<T>> {
            loop {
                let (range, patterns) = self
                    .children
                    .iter_mut()
                    .find(|(range, _)| **range != PARENT_RANGE)?;

                let range = *range;
                // TODO: use drain_filter to reduce pattern clones when stable
                let pattern = match patterns.keys().find(|pattern| !pattern.is_empty()) {
                    Some(pattern) => pattern.clone(),
                    None => {
                        self.children.remove(&range);
                        continue;
                    }
                };

                let res = self
                    .children
                    .get_mut(&range)
                    .expect("checked the range")
                    .remove(&pattern)
                    .expect("checked the pattern");

                if self.children.get(&range).map_or(true, HashMap::is_empty) {
                    self.children.remove(&range);
                }

                return Some(res);
            }
        }

        fn try_mutate_into_next_child(mut self) -> Result<Self, Self> {
            if let Some(mut node) = self.take_next_child_node() {
                // Optimization: self.children.is_empty() means that this node has no children or parent left
                // thus there's no point in returning to it.
                if !self.children.is_empty() {
                    let _prev = node.set_parent(self);
                    debug_assert!(_prev.is_none());
                }

                Ok(node)
            } else {
                Err(self)
            }
        }
    }

    impl<T> IntoIterator for MemcmpNode<T> {
        type Item = (Memcmp, MemcmpNode<T>);
        type IntoIter = IntoIter<T>;

        fn into_iter(mut self) -> Self::IntoIter {
            let children = std::mem::take(&mut self.children);
            IntoIter {
                ranges_iter: children.into_iter(),
                pattern_iter: None,
            }
        }
    }

    pub struct IntoIter<T> {
        ranges_iter: HashMapIntoIter<Range, HashMap<Pattern, MemcmpNode<T>>>,
        pattern_iter: Option<(Range, HashMapIntoIter<Pattern, MemcmpNode<T>>)>,
    }

    impl<T> Iterator for IntoIter<T> {
        type Item = (Memcmp, MemcmpNode<T>);

        fn next(&mut self) -> Option<Self::Item> {
            loop {
                if self.pattern_iter.is_none() {
                    let (range, map) = self.ranges_iter.next()?;
                    self.pattern_iter.replace((range, map.into_iter()));
                }

                let (range, iter) = self.pattern_iter.as_mut().expect("checked none");
                match iter.next() {
                    None => self.pattern_iter = None,
                    Some((pattern, node)) => {
                        let memcmp = Memcmp {
                            offset: range.0,
                            bytes: pattern,
                        };
                        break Some((memcmp, node));
                    }
                }
            }
        }
    }

    impl<T> Default for MemcmpNode<T> {
        fn default() -> Self {
            Self {
                registered: None,
                children: HashMap::default(),
            }
        }
    }

    impl<T> Drop for MemcmpNode<T> {
        fn drop(&mut self) {
            while let Some(node) = self.take_next_child_node() {
                let mut current = Some(node);

                while let Some(node) = current.take() {
                    match node.try_mutate_into_next_child() {
                        Ok(child) => {
                            current.replace(child);
                        }
                        Err(mut node) => {
                            current = node.take_parent();
                            debug_assert!(node.children.is_empty());
                            drop(node);
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use bytes::Bytes;
    use proptest::collection::hash_set;
    use proptest::option;
    use proptest::prelude::*;
    use smallvec::smallvec;

    use super::*;

    const MAX_ACCOUNT_SIZE: usize = 10 * 1024 * 1024;
    const MAX_PATTERN_SIZE: usize = 128;

    // ===== Helper filter-related strategies =====

    prop_compose! {
        fn account(max_size: usize)
                  (vec in prop::collection::vec(any::<u8>(), 1..max_size)) -> AccountData
        {
            AccountData {
                data: Bytes::from(vec)
            }
        }
    }

    fn is_none<T>(opt: Option<T>) -> usize {
        opt.as_ref().map_or(1, |_| 0)
    }

    // ---- Memcmp -----
    prop_compose! {
        fn arb_range()(pattern_len in 1..=MAX_PATTERN_SIZE)
                      (offset in 0..(MAX_ACCOUNT_SIZE - pattern_len), pattern_len in Just(pattern_len))
                      -> (usize, usize)
        {
            (offset, (offset + pattern_len))
        }
    }

    prop_compose! {
        fn valid_ranges(data_len: usize)(offset in 0..(data_len - 1))
                       (offset in Just(offset), slice_len in 1..(data_len - offset).min(128))
                       -> (usize, usize) {
            (offset, slice_len)
        }
    }

    prop_compose! {
        fn memcmp_from_range(range: (usize, usize))
                            (data in prop::collection::vec(any::<u8>(), range.1 - range.0))
                        -> Memcmp {
            Memcmp {
                offset: range.0,
                bytes: Pattern::from(data),
            }
        }
    }

    prop_compose! {
        fn valid_memcmp(account: AccountData)
                       ((offset, slice_len) in valid_ranges(account.len()))
                        -> Memcmp
        {
            let slice = account.data.get(offset..(offset + slice_len)).unwrap();
            Memcmp {
                offset,
                bytes: Pattern::from_slice(slice),
            }
        }
    }

    // ----- Filters -----
    fn arb_filters(max_memcmps: usize) -> impl Strategy<Value = Filters> {
        let data_size = option::of(any::<u64>());
        let strat = data_size.prop_flat_map(move |data_size| {
            let memcmps = hash_set(
                arb_range().prop_flat_map(memcmp_from_range),
                is_none(data_size)..max_memcmps,
            );
            (Just(data_size), memcmps)
        });

        strat.prop_map(|(data_size, memcmps)| {
            let raw = data_size
                .map(Filter::DataSize)
                .into_iter()
                .chain(memcmps.into_iter().map(Filter::Memcmp));
            Filters::new_normalized(raw).unwrap()
        })
    }

    prop_compose! {
        fn arb_non_matching_filters(max_memcmps: usize, data: AccountData)
                                   (filters in arb_filters(max_memcmps).prop_filter(
                                        "Filter matched",
                                        move |f| !f.matches(&data),
                                   ))
                                   -> Filters
        { filters }
    }

    prop_compose! {
        fn valid_filters(acc: AccountData, max_memcmps: usize)
                        (data_size in prop::option::of(Just(acc.len() as u64)))
                        (memcmps in hash_set(valid_memcmp(acc.clone()), is_none(data_size)..max_memcmps),
                         data_size in Just(data_size))
                        -> Filters
        {
            let raw = data_size.map(Filter::DataSize).into_iter().chain(
                memcmps.into_iter().map(Filter::Memcmp)
            );
            Filters::new_normalized(raw).unwrap()
        }
    }

    // ===== HashMap Operation =====

    #[derive(Debug)]
    enum HashMapOp<K, V> {
        Insert((K, V)),
        Remove(K),
    }

    fn arb_ops<K: Strategy + Clone, V: Strategy>(
        key_strat: K,
        value_strat: V,
    ) -> impl Strategy<Value = HashMapOp<K::Value, V::Value>> {
        prop_oneof![
            (key_strat.clone(), value_strat).prop_map(HashMapOp::Insert),
            key_strat.prop_map(HashMapOp::Remove)
        ]
    }

    // ===== Strategies for generating test parameters =====

    prop_compose! {
        /// Generate an accout and arbitrary matching filter groups
        fn acc_and_valid_filters(max_acc_size: usize, max_depth: usize, max_filters: usize)
                                (acc in account(max_acc_size))
                                (acc in Just(acc.clone()),
                                 groups in hash_set(valid_filters(acc, max_depth), 0..max_filters))
                                -> (AccountData, HashSet<Filters>)
        { (acc, groups) }
    }

    prop_compose! {
        /// Generate an accout and arbitrary filter groups (both matching and non-matching)
        fn acc_and_arb_filters(max_acc_size: usize, max_depth: usize, max_good: usize, max_bad: usize)
                              ((acc, good) in acc_and_valid_filters(max_acc_size, max_depth, max_good))
                              (bad in hash_set(arb_non_matching_filters(max_depth, acc.clone()), 0..max_bad),
                               acc in Just(acc), good in Just(good))
                              -> (AccountData, HashSet<Filters>, HashSet<Filters>)
        { (acc, good, bad) }
    }

    prop_compose! {
        /// Generate a set of filters and a sequence of insert-remove operations
        /// that use filters from the set as keys
        fn arb_hashmap_ops(max_depth: usize, max_groups: usize, max_ops: usize)
                          (groups in hash_set(arb_filters(max_depth), 1..max_groups))
                          (ops in prop::collection::vec(
                               arb_ops(0..groups.len(), any::<usize>()),
                               0..max_ops,
                           ),
                           groups in Just(groups))
                          -> (HashSet<Filters>, Vec<HashMapOp<usize, usize>>)
        { (groups, ops) }
    }

    // ===== Proptests =====

    proptest! {
        #[test]
        /// Tests that [`FilterTree`] constructed only from filter-groups that match an account
        /// returns all filters contained within a tree in a [`FiltersTree::map_matches`] call
        fn test_matches_valid_filters(
            (acc, filter_groups) in acc_and_valid_filters(128 * 1024, 3, 300)
        ) {
            let mut tree = FilterTree::new();
            for group in &filter_groups {
                tree.insert(group.clone(), ());
            }

            let matched = collect_all_matches(&tree, &acc);
            assert_eq!(matched.len(), filter_groups.len());
            let matched: HashSet<_> = matched.into_iter().collect();
            assert_eq!(matched, filter_groups);
        }

        #[test]
        /// Tests that [`FilterTree::map_matches`] will return matching
        /// and will not return non-matching filter groups
        fn test_matches_overall(
            (acc, good_filters, bad_filters)
                in acc_and_arb_filters(1024, 12, 200, 200)
        ) {
            let mut tree = FilterTree::new();
            for group in good_filters.iter().chain(&bad_filters) {
                tree.insert(group.clone(), ());
            }

            let matched = collect_all_matches(&tree, &acc);
            assert_eq!(matched.len(), good_filters.len());
            let matched: HashSet<_> = matched.into_iter().collect();
            assert_eq!(matched, good_filters);

            for group in good_filters {
                tree.remove(&group).unwrap();
            }

            let actual: Vec<Filters> = tree.into_iter().map(|(k, _)| k).collect();
            assert_eq!(actual.len(), bad_filters.len());
            let actual: HashSet<_> = actual.into_iter().collect();
            assert_eq!(actual, bad_filters);

        }

        #[test]
        /// Tests that [`FilterTree`] operates as a map in terms of insert and remove operations,
        /// as well as that remove do not drop or remove unrelated values or nodes.
        fn map((set, ops) in arb_hashmap_ops(24, 300, 10_000)) {
            let set: Vec<_> = set.into_iter().collect();
            let mut map = HashMap::new();
            let mut tree = FilterTree::new();

            #[derive(Default, PartialEq, Eq, Debug, Clone)]
            struct DropChecker(bool);
            impl Drop for DropChecker {
                fn drop(&mut self) {
                    assert!(self.0, "dropped without removal");
                }
            }

            fn disarm<T>((a, mut b): (T, DropChecker)) -> (T, DropChecker) {
                b.0 = true;
                (a, b)
            }

            ops.into_iter().for_each(|op| match op {
                HashMapOp::Insert((key, value)) => {
                    let filters = set.get(key).unwrap();
                    let expected = map
                        .insert(filters.clone(), (value, DropChecker::default()))
                        .map(disarm);
                    let actual = tree
                        .insert(filters.clone(), (value, DropChecker::default()))
                        .map(disarm);

                    assert_eq!(expected, actual);
                    assert_eq!(map.len(), tree.len());
                }
                HashMapOp::Remove(key) => {
                    let filters = set.get(key).unwrap();
                    let expected = map.remove(filters).map(disarm);
                    let actual = tree.remove(filters).map(disarm);
                    if let Some((expected, actual)) = expected.clone().zip(actual.clone()) {
                        assert_eq!(expected.0, actual.0);
                    } else {
                        assert_eq!(expected, actual);
                    }
                    assert_eq!(map.len(), tree.len());
                }
            });

            map.values_mut().for_each(|(_, checker)| checker.0 = true);
            let tree: HashMap<_, _> = tree.into_iter().map(|(k, v)| (k, disarm(v))).collect();
            assert_eq!(map, tree);
        }
    }

    // ===== Basic tests =====

    #[test]
    fn basic_functionality() {
        let filters = filters!(@size 20, @cmp 3: [1, 2, 3, 4]).unwrap();

        let mut tree = FilterTree::new();
        tree.insert(filters.clone(), ());

        let mut data = vec![0; 20];
        (1u8..8).for_each(|i| data[2 + i as usize] = i);
        let data = AccountData { data: data.into() };

        assert_eq!(collect_all_matches(&tree, &data), vec![filters.clone()]);

        let filters2 = filters!(@size 20, @cmp 5: [3, 4, 5, 6, 7]).unwrap();
        tree.insert(filters2.clone(), ());

        let mut expected: HashSet<_> = vec![filters.clone(), filters2].into_iter().collect();
        let actual: HashSet<_> = collect_all_matches(&tree, &data).into_iter().collect();
        assert_eq!(expected, actual);

        // A couple of bad filters
        let filters3 = filters!(@size 19, @cmp 5: [3, 4, 5, 6, 7]).unwrap();
        let filters4 = filters!(@size 20, @cmp 5: [3, 4, 5, 5, 7]).unwrap();
        tree.insert(filters3, ());
        tree.insert(filters4, ());

        let actual: HashSet<_> = collect_all_matches(&tree, &data).into_iter().collect();
        assert_eq!(expected, actual);

        // Insert a bunch of bad filters of different range into the same path as the filter6
        // to check that iter implementation correctly traverses all ranges
        // Note: This does not always fail when should
        let mut filters5 = filters!(@size 20, @cmp 3: [1, 2, 3], @cmp 7: [0, 0]).unwrap();
        for i in 1..200 {
            filters5.memcmp.last_mut().unwrap().bytes = vec![0; i].into();
            tree.insert(filters5.clone(), ());
        }

        let filters6 = filters!(@size 20, @cmp 3: [1, 2, 3], @cmp 7: [ 5, 6, 7]).unwrap();
        tree.insert(filters6.clone(), ());
        expected.insert(filters6);
        let actual: HashSet<_> = collect_all_matches(&tree, &data).into_iter().collect();
        assert_eq!(expected, actual);

        assert!(tree.remove(&filters).is_some());
        expected.remove(&filters);
        let actual: HashSet<_> = collect_all_matches(&tree, &data).into_iter().collect();
        assert_eq!(actual, expected);
        // No double removal
        assert!(tree.remove(&filters).is_none());

        // false on non-existent remove
        assert!(FilterTree::<()>::new().remove(&filters).is_none());
    }

    #[test]
    fn basic_into_iter() {
        let groups = vec![
            filters!(@cmp 13: [2, 4, 5, 7, 2, 4], @size 42, @cmp 1098: [3, 4, 3, 3, 3]).unwrap(),
            filters!(@size 19, @cmp 5: [3, 4, 5, 6, 7]).unwrap(),
            filters!(@size 28, @cmp 42: [23, 23, 23]).unwrap(),
            filters!(@size 28).unwrap(),
            filters!(@cmp 42: [23, 23, 23]).unwrap(),
        ];
        let mut groups: HashSet<_> = groups.into_iter().collect();

        let mut tree = FilterTree::new();
        groups.iter().cloned().for_each(|filters| {
            tree.insert(filters, ());
        });

        // basic
        let actual: HashSet<_> = tree.into_iter().map(|(f, _)| f).collect();
        assert_eq!(actual, groups);

        let mut tree = FilterTree::new();
        groups.iter().cloned().for_each(|filters| {
            tree.insert(filters, ());
        });

        // don't leak
        groups.remove(&filters!(@size 28, @cmp 42: [23, 23, 23]).unwrap());
        tree.remove(&filters!(@size 28, @cmp 42: [23, 23, 23]).unwrap());
        let actual: HashSet<_> = tree.clone().into_iter().map(|(f, _)| f).collect();
        assert_eq!(actual, groups);

        assert!(groups.remove(&filters!(@size 28).unwrap()));
        assert!(tree.remove(&filters!(@size 28).unwrap()).is_some());
        let actual: HashSet<_> = tree.into_iter().map(|(f, _)| f).collect();
        assert_eq!(actual, groups);

        // empty tree => empty IntoIter
        assert!(FilterTree::<()>::new().into_iter().next().is_none());
    }

    #[test]
    fn dont_blow_the_stack() {
        let mut filters = filters!(@size 128).unwrap();
        let mut tree = FilterTree::new();

        filters.memcmp.reserve(1024 * 10 + 1);

        for i in 0..1024 * 128 {
            // This must not happen in reality since we deduplicate and sort all filters
            filters.memcmp.push(Memcmp {
                offset: i % 12,
                bytes: smallvec![0, 0, 0],
            });
        }
        tree.insert(filters.clone(), ());

        filters.memcmp[10].bytes[1] = 42;
        tree.insert(filters.clone(), ());

        filters.memcmp[50].offset = 80;
        tree.insert(filters.clone(), ());

        filters.memcmp[50].offset = 45;
        tree.insert(filters.clone(), ());

        filters.data_size.replace(429);
        tree.insert(filters, ());

        let data = vec![0; 128];
        let data = AccountData { data: data.into() };
        tree.map_matches(&data, |_| ());

        eprintln!("started drop");
        drop(tree);
    }
}
