#[macro_use]
extern crate clap;
use clap::{App};

use std::path::PathBuf;


fn main(){
    let yaml = load_yaml!("cli_kvs.yml"); // static checking.
    let matc = App::from_yaml(&yaml).get_matches();
    let mut kvs = rskvs::KvStore::open(
        [".", "stab"].iter().collect::<PathBuf>()
    ).unwrap();
    if let Err(e) = kvs.rebuild(){
        eprintln!("{}", e);
        std::process::exit(-1);
    }

    match matc.subcommand(){
        ("set", Some(sub_set)) => {
            let key = sub_set.value_of("KEY").unwrap_or("");
            let val = sub_set.value_of("VALUE").unwrap_or("");
            eprintln!("{} - {}", &key, &val);
            if let Err(e) = kvs.set(key.to_string(), val.to_string()){
                eprintln!("--{}", e);
                std::process::exit(-1);
            }
        },
        ("get", Some(sub_get)) => {
            let key = sub_get.value_of("KEY").unwrap_or("");
            if let Ok(sv) = kvs.get(key.to_string()){
                match sv{
                    None => eprintln!("Key not found"),
                    Some(v) => eprintln!("{}", v),
                }
            }else{
                std::process::exit(-1);
            }
        },
        ("rm", Some(sub_rm)) => {
            let key = sub_rm.value_of("KEY").unwrap_or("");
            if let Err(_) = kvs.remove(key.to_string()){
                std::process::exit(-1);
            }
        },
        _ => {
        },
    }
    std::process::exit(0);
}