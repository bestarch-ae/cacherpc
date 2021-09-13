use std::collections::hash_map::IntoIter as HashMapIntoIter;
use std::collections::HashMap;

use smallvec::SmallVec;

use crate::types::AccountData;

use super::{Filter, Filters, Memcmp, Pattern, Range};

use node::MemcmpNode;

type Registered<T> = Option<T>;

pub(crate) struct FilterTree<T> {
    // None is for filter groups that do not contain datasize filter
    data_size: HashMap<Option<u64>, MemcmpNode<T>>,
}

impl<T> FilterTree<T> {
    pub fn new() -> Self {
        Self {
            data_size: HashMap::new(),
        }
    }

    pub fn insert(&mut self, filters: Filters, value: T) {
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

        node.registered = Some(value);
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

impl<T> IntoIterator for FilterTree<T> {
    type Item = (Filters, T);
    type IntoIter = IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter {
            roots_iter: self.data_size.into_iter(),
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
    use backoff::default;

    use super::*;

    pub struct MemcmpNode<T> {
        pub(super) registered: Registered<T>,
        pub(super) children: HashMap<Range, HashMap<Pattern, MemcmpNode<T>>>,
    }

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
    }

    impl<T> IntoIterator for MemcmpNode<T> {
        type Item = (Memcmp, MemcmpNode<T>);
        type IntoIter = IntoIter<T>;

        fn into_iter(self) -> Self::IntoIter {
            IntoIter {
                ranges_iter: self.children.into_iter(),
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
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    fn all_matches<T>(tree: &FilterTree<T>, data: &AccountData) -> Vec<Filters> {
        let mut vec = Vec::new();
        let closure = |filters| vec.push(filters);

        tree.map_matches(data, closure);
        vec
    }

    #[test]
    fn basic_functionality() {
        let filters = filters!(@size 20, @cmp 3: [1, 2, 3, 4]).unwrap();

        let mut tree = FilterTree::new();
        tree.insert(filters.clone(), ());

        let mut data = vec![0; 20];
        (1u8..8).for_each(|i| data[2 + i as usize] = i);
        let data = AccountData { data: data.into() };

        assert_eq!(all_matches(&tree, &data), vec![filters.clone()]);

        let filters2 = filters!(@size 20, @cmp 5: [3, 4, 5, 6, 7]).unwrap();
        tree.insert(filters2.clone(), ());

        let expected: HashSet<_> = vec![filters.clone(), filters2.clone()]
            .into_iter()
            .collect();
        let actual: HashSet<_> = all_matches(&tree, &data).into_iter().collect();
        assert_eq!(expected, actual);

        // A couple of bad filters
        let filters3 = filters!(@size 19, @cmp 5: [3, 4, 5, 6, 7]).unwrap();
        let filters4 = filters!(@size 20, @cmp 5: [3, 4, 5, 5, 7]).unwrap();
        tree.insert(filters3.clone(), ());
        tree.insert(filters4.clone(), ());

        let actual: HashSet<_> = all_matches(&tree, &data).into_iter().collect();
        assert_eq!(expected, actual);

        tree.remove(&filters);
        assert_eq!(all_matches(&tree, &data), vec![filters2.clone()]);
    }
}
