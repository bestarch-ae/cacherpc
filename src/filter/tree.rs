use std::collections::hash_map::IntoIter as HashMapIntoIter;
use std::collections::HashMap;

use smallvec::SmallVec;

use crate::types::AccountData;

use super::{Filter, Filters, Memcmp, Pattern, Range, RESERVED_RANGE};

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

    #[allow(unused)]
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn remove(&mut self, filters: &Filters) -> Option<T> {
        let mut node = self.data_size.get_mut(&filters.data_size)?;

        let memcmp = &filters.memcmp;

        for filter in memcmp {
            let range = filter.range();
            let temp_node = node
                .children
                .get_mut(&range)
                .and_then(|nodes| nodes.get_mut(&filter.bytes));

            node = temp_node?;
        }

        if node.registered.is_some() {
            self.len = self.len.saturating_sub(1);
        }

        node.registered.take()
    }

    pub fn map_matches(&self, data: &AccountData, mut f: impl FnMut(Filters)) {
        let data_len = data.len() as u64;

        for (data_size, node) in [Some(data_len), None]
            .iter()
            .filter_map(|k| Some(*k).zip(self.data_size.get(k)))
        {
            let base = Filters {
                data_size,
                memcmp: SmallVec::new(),
            };

            node.map_matches(&base, data, &mut f);
        }
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

    impl<T> MemcmpNode<T> {
        pub fn map_matches(&self, base: &Filters, data: &AccountData, f: &mut impl FnMut(Filters)) {
            // f is &mut so that the compiler wont fail because of recursion limit
            if self.registered.is_some() {
                f(base.clone())
            }

            let iter_nodes = self.children.iter().filter_map(|((from, to), nodes)| {
                // For each available range
                data.data
                    .get(*from..*to) // Get dataslice
                    .and_then(|slice| nodes.get_key_value(slice)) // Get registered nodes matching this data
                    .map(|(slice, node)| (*from, slice.clone(), node))
            });

            for (offset, bytes, node) in iter_nodes {
                let mut new_base = base.clone();
                new_base.memcmp.push(Memcmp { offset, bytes });
                new_base.memcmp.sort_unstable();

                node.map_matches(&new_base, data, f);
            }
        }

        // The rest are helper methods for Drop implementation

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

    use smallvec::smallvec;

    use super::*;

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

        let expected: HashSet<_> = vec![filters.clone(), filters2.clone()]
            .into_iter()
            .collect();
        let actual: HashSet<_> = collect_all_matches(&tree, &data).into_iter().collect();
        assert_eq!(expected, actual);

        // A couple of bad filters
        let filters3 = filters!(@size 19, @cmp 5: [3, 4, 5, 6, 7]).unwrap();
        let filters4 = filters!(@size 20, @cmp 5: [3, 4, 5, 5, 7]).unwrap();
        tree.insert(filters3.clone(), ());
        tree.insert(filters4.clone(), ());

        let actual: HashSet<_> = collect_all_matches(&tree, &data).into_iter().collect();
        assert_eq!(expected, actual);

        assert!(tree.remove(&filters).is_some());
        assert_eq!(collect_all_matches(&tree, &data), vec![filters2.clone()]);
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
        tree.insert(filters.clone(), ());

        eprintln!("started drop");
        drop(tree);
    }
}
