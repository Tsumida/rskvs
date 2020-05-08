#[macro_use]
extern crate clap;
use clap::App;
use rskvs::KVStoreBuilder;

fn main() {
    let yaml = load_yaml!("cli_kvs.yml"); // static checking.
    let matc = App::from_yaml(&yaml).get_matches();
    let mut bd = KVStoreBuilder::new();
    if let Some(path) = matc.value_of("Dir") {
        bd.set_path(path);
    }

    let mut kvs = bd.set_data_threshold(512).build().unwrap();
    kvs.init_state();

    match matc.subcommand() {
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
