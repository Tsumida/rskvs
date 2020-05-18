use std::time::SystemTime;

#[macro_use]
extern crate clap;
use clap::App;
use tempfile::TempDir;

use rskvs::KvStoreBuilder;

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
}

fn main() {
    let yaml = load_yaml!("cli_kvs.yml"); // static checking.
    let matc = App::from_yaml(&yaml).get_matches();
    let mut bd = KvStoreBuilder::new();
    if let Some(path) = matc.value_of("dir") {
        bd.set_dir_path(path);
    }

    let mut kvs = bd.set_data_threshold(512).build().unwrap();
    kvs.init_state();

    match matc.subcommand() {
        ("bench", Some(sub_set)) => {
            let size = 20000;
            eprintln!("case size = {}", size);
            let v = test_case(size);
            let random_index = (0..size)
                .map(|i| rand::random::<usize>() % size)
                .collect::<Vec<usize>>();
            if sub_set.is_present("WRITE_NO_COMPRESSION") {
                eprintln!("testing writing without compression...");
                eprintln!("take: {} ms", bench_write_no_compression(&v));
            }

            if sub_set.is_present("WRITE") {
                eprintln!("testing writing with compression...");
                eprintln!("take: {} ms", bench_write(&v));
            }

            if sub_set.is_present("READ") {
                eprintln!("testing reading...");
                eprintln!("take: {} ms", bench_read(&v, &random_index));
            }

            std::process::exit(0);
        }
        ("set", Some(sub_set)) => {
            let key = sub_set.value_of("KEY").unwrap_or("");
            let val = sub_set.value_of("VALUE").unwrap_or("");
            if let Err(e) = kvs.set(key.to_string(), val.to_string()) {
                println!("--{}", e);
                std::process::exit(-1);
            }
            std::process::exit(0);
        }
        ("get", Some(sub_get)) => {
            let key = sub_get.value_of("KEY").unwrap_or("");
            match kvs.get(key.to_string()) {
                Ok(sv) => {
                    match sv {
                        None => println!("Key not found"),
                        Some(v) => println!("{}", v),
                    }
                    std::process::exit(0);
                }
                Err(e) => {
                    println!("{}", e);
                    std::process::exit(-1);
                }
            }
        }
        ("rm", Some(sub_rm)) => {
            let key = sub_rm.value_of("KEY").unwrap_or("");
            match kvs.remove(key.to_string()) {
                Ok(r) => {
                    if !r {
                        println!("Key not found");
                        std::process::exit(-1);
                    }
                    std::process::exit(0);
                }
                Err(_) => std::process::exit(-1),
            }
        }
        _ => {
            std::process::exit(-1);
        }
    }
}
