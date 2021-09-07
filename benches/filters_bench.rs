use bytes::Bytes;
use cache_rpc::filter::Filter;
use cache_rpc::types::{AccountData, AccountInfo};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use rand::prelude::*;
use smallvec::SmallVec;
use std::collections::HashSet;

fn test_data(size: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    (0..size).map(|_| rng.gen()).collect()
}

/*
fn test_filter(limit: usize) -> Filter {
    let mut rng = rand::thread_rng();
    if rng.gen() {
        Filter::DataSize(rng.gen())
    } else {
        Filter::Memcmp {
            offset: rng.gen_range(0..limit),
            bytes: (0..rng.gen_range(0..limit)).map(|_| rng.gen()).collect(),
        }
    }
}
*/

fn test_filter_group(bytes: &[u8]) -> SmallVec<[Filter; 2]> {
    let limit = bytes.len();
    let mut rng = rand::thread_rng();
    let offset = rng.gen_range(0..limit);
    let end = rng.gen_range(offset / 2..limit).min(offset + 128);
    let slice = bytes
        .get(offset..end)
        .map(SmallVec::<[u8; 128]>::from)
        .unwrap_or_else(|| {
            (0..rng.gen_range(0..limit.min(128)))
                .map(|_| rng.gen())
                .collect()
        });

    let f1 = Filter::Memcmp {
        offset,
        bytes: slice,
    };
    let f2 = Filter::DataSize(rng.gen_range(limit / 2..(limit * 2)) as u64);
    SmallVec::from([f2, f1])
}

fn filter_table(account_bytes: &[u8], count: usize) -> HashSet<SmallVec<[Filter; 2]>> {
    (0..count)
        .map(|_| test_filter_group(account_bytes))
        .collect()
}

fn ruleset_path() -> String {
    std::env::var("RULESET").expect("missing env RULESET")
}

fn data_path() -> String {
    std::env::var("DATA").expect("missing env DATA")
}

fn load_ruleset_from_json(path: &str) -> HashSet<SmallVec<[Filter; 2]>> {
    use serde::Deserialize;
    use serde_json::value::RawValue;
    use std::fs::File;
    use std::io::BufRead;

    let mut rules = HashSet::new();
    let f = File::open(path).expect("ruleset file can't be opened");
    let reader = std::io::BufReader::new(f);
    for line in reader.lines() {
        let line = line.unwrap();
        #[derive(Deserialize)]
        struct Request<'a> {
            #[serde(borrow)]
            params: &'a serde_json::value::RawValue,
        }
        #[derive(Deserialize)]
        struct Params {
            filters: SmallVec<[Filter; 2]>,
        }
        let req: Request<'_> = serde_json::from_str(&line).unwrap();
        let params: [&RawValue; 2] = serde_json::from_str(req.params.get()).unwrap();
        let params: Params = serde_json::from_str(params[1].get()).unwrap();
        let mut filters = params.filters;
        filters.sort_unstable();
        rules.insert(filters);
    }
    rules
}

fn load_data_from_json(path: &str) -> Vec<AccountData> {
    use serde::Deserialize;
    use std::fs::File;
    use std::io::BufRead;

    let mut data = Vec::new();
    let f = File::open(path).unwrap();
    let reader = std::io::BufReader::new(f);
    for line in reader.lines() {
        let line = line.unwrap();
        #[derive(Deserialize, Debug)]
        struct Notification {
            params: Params,
        }
        #[derive(Deserialize, Debug)]
        struct Value {
            account: AccountInfo,
        }
        #[derive(Deserialize, Debug)]
        struct Result {
            value: Value,
        }
        #[derive(Deserialize, Debug)]
        struct Params {
            result: Result,
        }

        let notification: Notification = serde_json::from_str(&line).unwrap();
        data.push(notification.params.result.value.account.data);
    }
    data
}

fn bench_filters_real_data(c: &mut Criterion) {
    use criterion::Throughput;

    let mut group = c.benchmark_group("Filters/Real");

    let data = load_data_from_json(&data_path());
    let filter_table = load_ruleset_from_json(&ruleset_path());

    group.throughput(Throughput::Elements(data.len() as u64));

    group.bench_function(BenchmarkId::new("Dumb", data.len()), |b| {
        b.iter(|| {
            for data in &data {
                dumb(data, filter_table.iter());
            }
        })
    });

    group.finish();
}

fn bench_filters(c: &mut Criterion) {
    let mut group = c.benchmark_group("Filters");

    let small_data = prepare("1kb data", 1024, |data| filter_table(data, 50_000));
    let big_data = prepare("1mb data", 1024 * 1024, |data| filter_table(data, 50_000));
    let file_data = prepare("1mb data, from file", 1024 * 1024, |_| {
        load_ruleset_from_json(&ruleset_path())
    });

    // sanity check
    for i in [small_data, big_data, file_data].iter() {
        group.bench_with_input(BenchmarkId::new("Dumb", i.0), i, |b, (_, data, filters)| {
            b.iter(|| dumb(black_box(data), filters.iter()))
        });
    }
    group.finish();
}

fn prepare(
    name: &'static str,
    data_size: usize,
    gen_table: impl Fn(&[u8]) -> HashSet<SmallVec<[Filter; 2]>>,
) -> (&'static str, AccountData, HashSet<SmallVec<[Filter; 2]>>) {
    let data = AccountData {
        data: Bytes::from(test_data(data_size)),
    };
    let filter_table = gen_table(&data.data[..]);
    let dumb_count = dumb(&data, filter_table.iter());

    println!(
        "{} rules: {} matches: {}",
        name,
        filter_table.len(),
        dumb_count
    );

    (name, data, filter_table)
}

fn dumb<'a>(data: &AccountData, table: impl Iterator<Item = &'a SmallVec<[Filter; 2]>>) -> usize {
    let mut matches = 0;
    for group in table {
        if group.iter().all(|f| f.matches(data)) {
            matches += 1;
        }
    }
    matches
}

criterion_group!(benches, bench_filters, bench_filters_real_data);
criterion_main!(benches);
