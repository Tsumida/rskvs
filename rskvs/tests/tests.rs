use assert_cmd::prelude::*;
use predicates::ord::eq;
use predicates::str::{contains, is_empty, PredicateStrExt};
use rskvs::{KvStore, KvStoreBuilder, Result};
use std::process::Command;
use tempfile::TempDir;
use walkdir::WalkDir;

// `kvs` with no args should exit with a non-zero code.
#[test]
fn cli_no_args() {
    let temp_dir = TempDir::new().unwrap();
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["-s", temp_dir.path().to_str().unwrap()])
        .assert()
        .failure();
}

// `kvs -V` should print the version
#[test]
fn cli_version() {
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["-V"])
        .assert()
        .stdout(contains(env!("CARGO_PKG_VERSION")));
}

// `kvs get <KEY>` should print "Key not found" for a non-existent key and exit with zero.
#[test]
fn cli_get_non_existent_key() {
    let temp_dir = TempDir::new().unwrap();
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["-s", temp_dir.path().to_str().unwrap()])
        .args(&["get", "key1"])
        .assert()
        .success()
        .stdout(contains("Key not found").trim());
}

// `kvs rm <KEY>` should print "Key not found" for an empty database and exit with non-zero code.
#[test]
fn cli_rm_non_existent_key() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["-s", temp_dir.path().to_str().unwrap()])
        .args(&["rm", "key1"])
        .assert()
        .failure()
        .stdout(eq("Key not found").trim());
}

// `kvs set <KEY> <VALUE>` should print nothing and exit with zero.
#[test]
fn cli_set() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["-s", temp_dir.path().to_str().unwrap()])
        .args(&["set", "key1", "value1"])
        .assert()
        .success()
        .stdout(is_empty());
}

#[test]
fn cli_get_stored() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");

    let mut store = KvStore::open(temp_dir.path())?;
    store.set("key1".to_owned(), "value1".to_owned())?;
    store.set("key2".to_owned(), "value2".to_owned())?;
    drop(store);

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["-s", temp_dir.path().to_str().unwrap()])
        .args(&["get", "key1"])
        .assert()
        .success()
        .stdout(eq("value1").trim());

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["-s", temp_dir.path().to_str().unwrap()])
        .args(&["get", "key2"])
        .assert()
        .success()
        .stdout(eq("value2").trim());

    Ok(())
}

// `kvs rm <KEY>` should print nothing and exit with zero.
#[test]
fn cli_rm_stored() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");

    let mut store = KvStore::open(temp_dir.path())?;
    store.set("key1".to_owned(), "value1".to_owned())?;
    drop(store);

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["-s", temp_dir.path().to_str().unwrap()])
        .args(&["rm", "key1"])
        .assert()
        .success()
        .stdout(is_empty());

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["-s", temp_dir.path().to_str().unwrap()])
        .args(&["get", "key1"])
        .assert()
        .success()
        .stdout(eq("Key not found").trim());

    Ok(())
}

#[test]
fn cli_invalid_get() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["-s", temp_dir.path().to_str().unwrap()])
        .args(&["get"])
        .assert()
        .failure();

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["-s", temp_dir.path().to_str().unwrap()])
        .args(&["get", "extra", "field"])
        .assert()
        .failure();
}

#[test]
fn cli_invalid_set() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["-s", temp_dir.path().to_str().unwrap()])
        .args(&["set"])
        .assert()
        .failure();

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["-s", temp_dir.path().to_str().unwrap()])
        .args(&["set", "missing_field"])
        .assert()
        .failure();

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["-s", temp_dir.path().to_str().unwrap()])
        .args(&["set", "extra", "extra", "field"])
        .assert()
        .failure();
}

#[test]
fn cli_invalid_rm() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["-s", temp_dir.path().to_str().unwrap()])
        .args(&["rm"])
        .assert()
        .failure();

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["-s", temp_dir.path().to_str().unwrap()])
        .args(&["rm", "extra", "field"])
        .assert()
        .failure();
}

#[test]
fn cli_invalid_subcommand() {
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["unknown", "subcommand"])
        .assert()
        .failure();
}

// Should get previously stored value.
#[test]
fn get_stored_value() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path())?;

    store.set("key1".to_owned(), "value1".to_owned())?;
    store.set("key2".to_owned(), "value2".to_owned())?;

    assert_eq!(store.get("key1".to_owned())?, Some("value1".to_owned()));
    assert_eq!(store.get("key2".to_owned())?, Some("value2".to_owned()));

    // Open from disk again and check persistent data.
    drop(store);
    let mut store = KvStore::open(temp_dir.path())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value1".to_owned()));
    assert_eq!(store.get("key2".to_owned())?, Some("value2".to_owned()));

    Ok(())
}

// Should overwrite existent value.
#[test]
fn overwrite_value() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path())?;

    store.set("key1".to_owned(), "value1".to_owned())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value1".to_owned()));
    store.set("key1".to_owned(), "value2".to_owned())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value2".to_owned()));

    // Open from disk again and check persistent data.
    drop(store);
    let mut store = KvStore::open(temp_dir.path())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value2".to_owned()));
    assert_eq!(store.get("key1".to_owned())?, Some("value2".to_owned()));

    store.init_state();
    store.set("key1".to_owned(), "value3".to_owned())?;
    store.init_state();
    assert_eq!(store.get("key1".to_owned())?, Some("value3".to_owned()));

    Ok(())
}

// Should get `None` when getting a non-existent key.
#[test]
fn get_non_existent_value() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path())?;

    store.set("key1".to_owned(), "value1".to_owned())?;
    assert_eq!(store.get("key2".to_owned())?, None);

    // Open from disk again and check persistent data.
    drop(store);
    let mut store = KvStore::open(temp_dir.path())?;
    assert_eq!(store.get("key2".to_owned())?, None);

    Ok(())
}

#[test]
fn remove_non_existent_key() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path())?;
    assert_eq!(false, store.remove("key1".to_owned()).unwrap());
    Ok(())
}

#[test]
fn remove_key() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path())?;
    store.set("key1".to_owned(), "value1".to_owned())?;
    assert_eq!(store.remove("key1".to_owned()).unwrap(), true);
    assert_eq!(store.get("key1".to_owned())?, None);
    Ok(())
}

// Insert data until total size of the directory decreases.
// Test data correctness after compaction.
#[test]
fn compression() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");

    let mut store = KvStoreBuilder::new()
        .set_dir_path(temp_dir.path())
        .set_data_threshold(1 << 15)
        .build()?;

    store.init_state();

    for iter in 0..10 {
        for key_id in 0..1000 {
            let key = format!("key{}", key_id);
            let value = format!("{}", iter);
            println!("keyid = {}", key_id);
            store.set(key, value)?;
        }
        store.init_state();
        // reopen and check content.
        for key_id in 0..1000 {
            let key = format!("key{}", key_id);
            eprintln!("check keyid = {}", key_id);
            assert_eq!(store.get(key)?, Some(format!("{}", iter)));
        }
    }
    Ok(())
}
