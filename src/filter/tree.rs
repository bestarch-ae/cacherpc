use std::collections::HashMap;

use serde::Deserialize;
use smallvec::{smallvec, SmallVec};

use crate::types::AccountData;

use super::{Filter, Filters, Memcmp, Pattern, Range};

type Registered = bool;

pub(crate) struct FilterTree {
    // None is for filter groups that do not contain datasize filter
    data_size: HashMap<Option<u64>, node::MemcmpNode>,
}

impl FilterTree {
    pub fn new() -> Self {
        Self {
            data_size: HashMap::new(),
        }
    }

    pub fn insert(&mut self, filters: Filters) {
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

        node.registered = true;
    }

    pub fn remove(&mut self, filters: &Filters) {
        let mut node = match self.data_size.get_mut(&filters.data_size) {
            Some(node) => node,
            None => return,
        };

        let memcmp = &filters.memcmp;

        for filter in memcmp {
            let range = filter.range();
            let temp_node = node
                .children
                .get_mut(&range)
                .and_then(|nodes| nodes.get_mut(&filter.bytes));

            node = match temp_node {
                Some(node) => node,
                None => return,
            };
        }

        node.registered = false;
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

mod node {
    use super::*;

    #[derive(Default)]
    pub(super) struct MemcmpNode {
        pub(super) registered: Registered,
        pub(super) children: HashMap<Range, HashMap<Pattern, MemcmpNode>>,
    }

    impl MemcmpNode {
        pub fn map_matches(&self, base: &Filters, data: &AccountData, f: &mut impl FnMut(Filters)) {
            // f is &mut so that the compiler wont fail because of recursion limit
            if self.registered {
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
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    fn all_matches(tree: &FilterTree, data: &AccountData) -> Vec<Filters> {
        let mut vec = Vec::new();
        let closure = |filters| vec.push(filters);

        tree.map_matches(data, closure);
        vec
    }

    #[test]
    fn basic_functionality() {
        let filters = filters!(@size 20, @cmp 3: [1, 2, 3, 4]).unwrap();

        let mut tree = FilterTree::new();
        tree.insert(filters.clone());

        let mut data = vec![0; 20];
        (1u8..8).for_each(|i| data[2 + i as usize] = i);
        let data = AccountData { data: data.into() };

        assert_eq!(all_matches(&tree, &data), vec![filters.clone()]);

        let filters2 = filters!(@size 20, @cmp 5: [3, 4, 5, 6, 7]).unwrap();
        tree.insert(filters2.clone());

        let expected: HashSet<_> = vec![filters.clone(), filters2.clone()]
            .into_iter()
            .collect();
        let actual: HashSet<_> = all_matches(&tree, &data).into_iter().collect();
        assert_eq!(expected, actual);

        // A couple of bad filters
        let filters3 = filters!(@size 19, @cmp 5: [3, 4, 5, 6, 7]).unwrap();
        let filters4 = filters!(@size 20, @cmp 5: [3, 4, 5, 5, 7]).unwrap();
        tree.insert(filters3.clone());
        tree.insert(filters4.clone());

        let actual: HashSet<_> = all_matches(&tree, &data).into_iter().collect();
        assert_eq!(expected, actual);

        tree.remove(&filters);
        assert_eq!(all_matches(&tree, &data), vec![filters2.clone()]);
    }
}
