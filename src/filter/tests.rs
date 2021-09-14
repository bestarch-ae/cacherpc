use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use proptest::collection::hash_set;
use proptest::option;
use proptest::prelude::*;
use smallvec::smallvec;

use super::filters::*;
use super::tree::*;
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

// === Tree Tests ===
// ===== Proptests =====

proptest! {
    #[test]
    /// Tests that [`FilterTree`] constructed only from filter-groups that match an account
    /// returns all filters contained within a tree in a [`FiltersTree::map_matches`] call
    fn tree_matches_valid_filters(
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
    fn tree_matches_overall(
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
    fn tree_as_map((set, ops) in arb_hashmap_ops(24, 300, 10_000)) {
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
fn tree_basic_functionality() {
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
fn tree_into_iter() {
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
fn tree_dont_blow_the_stack() {
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

// === Filters tests ===

#[test]
fn filters_order() {
    let f1 = Filter::Memcmp(Memcmp {
        offset: 1,
        bytes: SmallVec::new(),
    });
    let f2 = Filter::DataSize(0);
    assert!(f2 < f1);
}

#[test]
fn deserialize_filters() {
    let filter: Filter = serde_json::from_str(
        r#"{"memcmp":{"offset":13,"bytes":"HWHvQhFmJB3NUcu1aihKmrKegfVxBEHzwVX6yZCKEsi1"}}"#,
    )
    .unwrap();
    let expected = Filter::Memcmp(Memcmp {
        offset: 13,
        bytes: smallvec::smallvec![
            245, 59, 247, 123, 252, 249, 77, 70, 227, 252, 215, 248, 153, 192, 98, 123, 28, 232,
            159, 173, 44, 138, 177, 107, 137, 62, 126, 139, 186, 244, 124, 90
        ],
    });
    assert_eq!(filter, expected);

    let filter: Filter = serde_json::from_str(r#"{"dataSize":42069}"#).unwrap();
    let expected = Filter::DataSize(42069);
    assert_eq!(filter, expected);
}

#[test]
fn normalize() {
    // Multiple datasizes always normalize to false
    assert_eq!(
        filters!(@size 420, @size 69, @size 420),
        Err(NormalizeError::DuplicateDataSize)
    );

    // Equal datasizes are stripped
    assert_eq!(
        filters!(@size 420, @size 420).unwrap(),
        filters!(@size 420).unwrap(),
    );

    // Order is determined
    let filters1 =
        filters!(@cmp 13: [2, 4, 5, 7, 2, 4], @size 42, @cmp 1098: [3, 4, 3, 3, 3]).unwrap();
    let filters2 =
        filters!(@cmp 1098: [3, 4, 3, 3, 3], @cmp 13: [2, 4, 5, 7, 2, 4], @size 42).unwrap();
    let filters3 =
        filters!(@size 42, @cmp 13: [2, 4, 5, 7, 2, 4], @cmp 1098: [3, 4, 3, 3, 3]).unwrap();

    assert_eq!(filters1, filters2);
    assert_eq!(filters2, filters3);
    assert_eq!(filters1, filters3);
}
