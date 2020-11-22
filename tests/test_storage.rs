#[macro_use]
extern crate log;

use tempfile::TempDir;
use velli_db::{LocalStorage, Result};

#[test]
fn store_value_and_restore() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary dir.");
    let mut store = LocalStorage::new(temp_dir.path().to_path_buf())
        .expect("unable to create LocalStorage object.");

    store.set(b"key1".to_vec(), b"value1".to_vec())?;
    store.set(b"key2".to_vec(), b"value2".to_vec())?;

    assert_eq!(store.get(b"key1".to_vec())?, Some(b"value1".to_vec()));
    assert_eq!(store.get(b"key2".to_vec())?, Some(b"value2".to_vec()));

    drop(store);

    let store = LocalStorage::new(temp_dir.path().to_path_buf())
        .expect("unable to create LocalStorage object.");
    assert_eq!(store.get(b"key1".to_vec())?, Some(b"value1".to_vec()));
    assert_eq!(store.get(b"key2".to_vec())?, Some(b"value2".to_vec()));

    Ok(())
}

#[test]
fn delete_value_and_restore() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary dir.");
    let mut store = LocalStorage::new(temp_dir.path().to_path_buf())
        .expect("unable to create LocalStorage object.");

    store.set(b"key1".to_vec(), b"value1".to_vec())?;

    assert_eq!(store.get(b"key1".to_vec())?, Some(b"value1".to_vec()));

    store.delete(b"key1".to_vec())?;

    assert_eq!(store.get(b"key1".to_vec())?, None);

    drop(store);

    let store = LocalStorage::new(temp_dir.path().to_path_buf())
        .expect("unable to create LocalStorage object.");
    assert_eq!(store.get(b"key1".to_vec())?, None);

    Ok(())
}

#[test]
fn overwrite_value_and_restore_repeatedly() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary dir.");
    let mut store = LocalStorage::new(temp_dir.path().to_path_buf())
        .expect("unable to create LocalStorage object.");

    store.set(b"key1".to_vec(), b"value1".to_vec())?;

    assert_eq!(store.get(b"key1".to_vec())?, Some(b"value1".to_vec()));

    store.set(b"key1".to_vec(), b"value2".to_vec())?;

    assert_eq!(store.get(b"key1".to_vec())?, Some(b"value2".to_vec()));

    drop(store);

    let store = LocalStorage::new(temp_dir.path().to_path_buf())
        .expect("unable to create LocalStorage object.");
    assert_eq!(store.get(b"key1".to_vec())?, Some(b"value2".to_vec()));

    drop(store);

    let store = LocalStorage::new(temp_dir.path().to_path_buf())
        .expect("unable to create LocalStorage object.");
    assert_eq!(store.get(b"key1".to_vec())?, Some(b"value2".to_vec()));

    drop(store);

    let store = LocalStorage::new(temp_dir.path().to_path_buf())
        .expect("unable to create LocalStorage object.");
    assert_eq!(store.get(b"key1".to_vec())?, Some(b"value2".to_vec()));
    Ok(())
}

#[test]
fn massive_set() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary dir.");
    info!("DB path: {}", temp_dir.path().to_string_lossy());
    let count = 10000;
    let mut store = LocalStorage::new(temp_dir.path().to_path_buf())
        .expect("unable to create LocalStorage object.");
    for i in 0..count {
        store.set(
            format!("key{}", i).as_bytes().to_vec(),
            format!("value{}", i).as_bytes().to_vec(),
        )?;
    }
    for i in 0..count {
        let k = format!("key{}", i);
        let val = store.get(k.as_bytes().to_vec())?;
        match val.clone() {
            Some(v) => {
                let s = String::from_utf8(v.clone()).unwrap();

                info!("{}:\t{}", k, s);
            }
            None => {
                info!("{}: None", k);
                panic!("{}", k);
            }
        }
        assert_eq!(val, Some(format!("value{}", i).as_bytes().to_vec()));
    }
    drop(store);
    let store = LocalStorage::new(temp_dir.path().to_path_buf())
        .expect("unable to create LocalStorage object.");
    for i in 0..count {
        let k = format!("key{}", i);
        let val = store.get(k.as_bytes().to_vec())?;
        match val.clone() {
            Some(v) => {
                let s = String::from_utf8(v.clone()).unwrap();

                info!("{}:\t{}", k, s);
            }
            None => {
                info!("{}: None", k);
                panic!("{}", k);
            }
        }
        assert_eq!(val, Some(format!("value{}", i).as_bytes().to_vec()));
    }
    Ok(())
}