use criterion::{criterion_group, criterion_main, Criterion};

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
    let vec_str = test_case(2000);
    c.bench_function("bench_continous_wirte", |b| {
        b.iter(|| {
            let temp_dir = TempDir::new().expect("unable to create temporary working directory");

            let mut kvs = KvStoreBuilder::new()
                .set_dir_path(temp_dir.path())
                .set_data_threshold(1 << 16)
                .build()
                .unwrap();

            for (k, v) in &vec_str {
                kvs.set(k.clone(), v.clone()).unwrap();
            }
        })
    });
}

pub fn bench_continous_write_disable_compression(c: &mut Criterion) {
    let vec_str = test_case(2000);

    c.bench_function("bench_continous_wirte_without_compaction", |b| {
        b.iter(|| {
            let temp_dir = TempDir::new().expect("unable to create temporary working directory");

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
    });
}

pub fn bench_sequential_read(c: &mut Criterion) {
    let vec_str = test_case(2000);
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    c.bench_function("bench_sequential_read", |b| {
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
    });
}

pub fn bench_random_read(c: &mut Criterion) {
    let vec_str = test_case(2000);
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let random_index = (0..2000)
        .map(|i| rand::random::<usize>() % 2000)
        .collect::<Vec<usize>>();
    c.bench_function("bench_random_read", |b| {
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
    });
}

criterion_group!(
    benches,
    bench_continous_write,
    bench_continous_write_disable_compression,
    bench_sequential_read,
    bench_random_read,
);
criterion_main!(benches);
