use std::time::SystemTime;

#[macro_use]
extern crate clap;
use clap::App;
use tempfile::TempDir;

use simplelog::*;

use rskvs::{Cmd, KvStoreBuilder};
/*
fn test_case(size: usize) -> Vec<(String, String)> {
    (0..size)
        .map(|i| {
            (
                format!("kkkkkkkk{:08}", i),
                format!(
                    "vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
                    vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
                    vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
                    vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
                    vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
                    vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
                    vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
                    vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
                    vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv{:08}",
                    i
                ),
            )
        })
        .collect::<Vec<(String, String)>>()
}

fn bench_read(v: &Vec<(String, String)>, r: &Vec<usize>) -> usize {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut kvs = KvStoreBuilder::new()
        .set_dir_path(temp_dir.path())
        .set_data_threshold(1 << 16)
        .build()
        .unwrap();
    for (k, v) in v {
        kvs.set(k.clone(), v.clone()).unwrap();
    }

    let st = SystemTime::now();

    for &k in r {
        kvs.get(v.get(k).unwrap().0.clone()).unwrap();
    }

    let dur = SystemTime::now().duration_since(st).unwrap().as_millis();
    dur as usize
}

fn bench_write(v: &Vec<(String, String)>) -> usize {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut kvs = KvStoreBuilder::new()
        .set_dir_path(temp_dir.path())
        .set_data_threshold(1 << 16)
        .build()
        .unwrap();
    let st = SystemTime::now();

    for (k, v) in v {
        kvs.set(k.clone(), v.clone()).unwrap();
    }
    let dur = SystemTime::now().duration_since(st).unwrap().as_millis();
    dur as usize
}

fn bench_write_no_compression(v: &Vec<(String, String)>) -> usize {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut kvs = KvStoreBuilder::new()
        .set_dir_path(temp_dir.path())
        .set_data_threshold(1 << 16)
        .enable_log_compress(false)
        .build()
        .unwrap();
    let st = SystemTime::now();

    for (k, v) in v {
        kvs.set(k.clone(), v.clone()).unwrap();
    }
    let dur = SystemTime::now().duration_since(st).unwrap().as_millis();
    dur as usize
}*/

fn main() {
    CombinedLogger::init(vec![TermLogger::new(
        LevelFilter::Debug,
        Config::default(),
        TerminalMode::Mixed,
    )])
    .unwrap();
    let (kva, mut kvs) = rskvs::KvStoreBuilder::new()
        .set_data_threshold(4 << 10)
        .build()
        .unwrap();
    let h1 = std::thread::spawn(move || {
        kvs.run();
        kvs.init_state();
    });

    let kv_pairs = (0..100)
        .map(|_| random_kv())
        .collect::<Vec<(String, String)>>();

    for i in 0..10 {
        for (key, val) in &kv_pairs {
            let (s, r) = std::sync::mpsc::sync_channel(1);
            kva.op_sender
                .send((Cmd::Set(key.clone(), val.clone()), s))
                .unwrap();
            r.recv().unwrap();
        }
        //std::thread::sleep(std::time::Duration::from_millis(500));
    }
    drop(kva);
    h1.join().unwrap();
}

fn cli() {
    let yaml = load_yaml!("cli_kvs.yml"); // static checking.
    let matc = App::from_yaml(&yaml).get_matches();
    let mut bd = KvStoreBuilder::new();
    if let Some(path) = matc.value_of("dir") {
        bd.set_dir_path(path);
    }

    let (_, mut kvs) = bd.set_data_threshold(512).build().unwrap();
    kvs.init_state();
}

fn random_kv() -> (String, String) {
    let n = 4;
    let k = (rand::random::<usize>() % (1 << 30)).to_string();
    let mut v = String::with_capacity(n * k.len());
    for _ in 0..n {
        v.extend(k.lines());
    }

    (k, v)
}
