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
