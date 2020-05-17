use criterion::{criterion_group, criterion_main, Benchmark, Criterion};

use rskvs::KvStoreBuilder;
use tempfile::TempDir;

fn test_case(size: usize) -> Vec<(String, String)> {
    (0..size)
        .map(|i| {
            (
                format!("kkkkkkkk{:08}", i),
                format!(
                    "vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
                    vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv{:08}",
                    i
                ),
            )
        })
        .collect::<Vec<(String, String)>>()
}

pub fn bench_continous_write(c: &mut Criterion) {
    c.bench(
        "bench_continous_wirte",
        Benchmark::new("bench_continous_wirte", |b| {
            let vec_str = test_case(20000);
            b.iter(|| {
                let temp_dir =
                    TempDir::new().expect("unable to create temporary working directory");

                let mut kvs = KvStoreBuilder::new()
                    .set_dir_path(temp_dir.path())
                    .set_data_threshold(1 << 16)
                    .build()
                    .unwrap();

                for (k, v) in &vec_str {
                    kvs.set(k.clone(), v.clone()).unwrap();
                }
            })
        })
        .sample_size(10),
    );
}

pub fn bench_continous_write_disable_compression(c: &mut Criterion) {
    c.bench(
        "bench_continous_wirte_without_compaction",
        Benchmark::new("bench_continous_wirte_without_compaction", |b| {
            let vec_str = test_case(20000);
            b.iter(|| {
                let temp_dir =
                    TempDir::new().expect("unable to create temporary working directory");

                let mut kvs = KvStoreBuilder::new()
                    .set_dir_path(temp_dir.path())
                    .set_data_threshold(1 << 16)
                    .enable_log_compress(false)
                    .build()
                    .unwrap();

                for (k, v) in &vec_str {
                    kvs.set(k.clone(), v.clone()).unwrap();
                }
            })
        })
        .sample_size(10),
    );
}

pub fn bench_sequential_read(c: &mut Criterion) {
    c.bench(
        "bench_sequential_read",
        Benchmark::new("bench_sequential_read", |b| {
            let vec_str = test_case(20000);
            let temp_dir = TempDir::new().expect("unable to create temporary working directory");
            let mut store = KvStoreBuilder::new()
                .set_dir_path(temp_dir.path())
                .set_data_threshold(1 << 16)
                .build()
                .unwrap();
            for (k, v) in &vec_str {
                store.set(k.clone(), v.clone()).unwrap();
            }
            drop(store);
            b.iter(|| {
                let mut kvs = KvStoreBuilder::new()
                    .set_dir_path(temp_dir.path())
                    .set_data_threshold(1 << 16)
                    .build()
                    .unwrap();

                for (k, _) in &vec_str {
                    kvs.get(k.clone()).unwrap();
                }
            })
        })
        .sample_size(10),
    );
}

pub fn bench_random_read(c: &mut Criterion) {
    c.bench(
        "bench_random_read",
        Benchmark::new("bench_random_read", |b| {
            let vec_str = test_case(20000);
            let temp_dir = TempDir::new().expect("unable to create temporary working directory");
            let random_index = (0..20000)
                .map(|i| rand::random::<usize>() % 20000)
                .collect::<Vec<usize>>();
            let mut store = KvStoreBuilder::new()
                .set_dir_path(temp_dir.path())
                .set_data_threshold(1 << 16)
                .build()
                .unwrap();
            for (k, v) in &vec_str {
                store.set(k.clone(), v.clone()).unwrap();
            }
            drop(store);
            b.iter(|| {
                let mut kvs = KvStoreBuilder::new()
                    .set_dir_path(temp_dir.path())
                    .set_data_threshold(1 << 16)
                    .build()
                    .unwrap();

                for &i in &random_index {
                    kvs.get(vec_str.get(i).unwrap().0.clone()).unwrap();
                }
            })
        })
        .sample_size(20),
    );
}

criterion_group!(
    benches,
    bench_continous_write,
    bench_continous_write_disable_compression,
    bench_sequential_read,
    bench_random_read,
);
criterion_main!(benches);
