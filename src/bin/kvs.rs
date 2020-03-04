#[macro_use]
extern crate clap;
use clap::{App};

fn main(){
    let yaml = load_yaml!("cli_kvs.yml"); // static checking.
    let matc = App::from_yaml(&yaml).get_matches();

    //match matc.value_of()
    match matc.subcommand(){
        ("set", Some(sub_set)) => {
            //let key = sub_set.value_of("KEY").unwrap_or("");
            //let val = sub_set.value_of("VALUE").unwrap_or("");
            eprintln!("unimplemented");

        },
        ("get", Some(sub_get)) => {
            //let key = sub_set.value_of("KEY").unwrap_or("");
            //let val = sub_set.value_of("VALUE").unwrap_or("");
            eprintln!("unimplemented");
        },
        ("rm", Some(sub_rm)) => {
            //let key = sub_set.value_of("KEY").unwrap_or("");
            //let val = sub_set.value_of("VALUE").unwrap_or("");
            eprintln!("unimplemented");
        },
        _ => {
        },
    }
    std::process::exit(-1);

}