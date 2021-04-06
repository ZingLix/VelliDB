#[macro_use]
extern crate log;

use tempfile::TempDir;
use velli_db::{LocalStorage, Result};

#[async_std::test]
async fn store_value_and_restore() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary dir.");
    let mut store = LocalStorage::new(temp_dir.path().to_path_buf())
        .await
        .expect("unable to create LocalStorage object.");

    store.set(b"key1".to_vec(), b"value1".to_vec()).await?;
    store.set(b"key2".to_vec(), b"value2".to_vec()).await?;

    assert_eq!(store.get(b"key1".to_vec()).await?, Some(b"value1".to_vec()));
    assert_eq!(store.get(b"key2".to_vec()).await?, Some(b"value2".to_vec()));

    drop(store);

    let store = LocalStorage::new(temp_dir.path().to_path_buf())
        .await
        .expect("unable to create LocalStorage object.");
    assert_eq!(store.get(b"key1".to_vec()).await?, Some(b"value1".to_vec()));
    assert_eq!(store.get(b"key2".to_vec()).await?, Some(b"value2".to_vec()));

    Ok(())
}

#[async_std::test]
async fn delete_value_and_restore() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary dir.");
    let mut store = LocalStorage::new(temp_dir.path().to_path_buf())
        .await
        .expect("unable to create LocalStorage object.");

    store.set(b"key1".to_vec(), b"value1".to_vec()).await?;

    assert_eq!(store.get(b"key1".to_vec()).await?, Some(b"value1".to_vec()));

    store.delete(b"key1".to_vec()).await?;

    assert_eq!(store.get(b"key1".to_vec()).await?, None);

    drop(store);

    let store = LocalStorage::new(temp_dir.path().to_path_buf())
        .await
        .expect("unable to create LocalStorage object.");
    assert_eq!(store.get(b"key1".to_vec()).await?, None);

    Ok(())
}

#[async_std::test]
async fn overwrite_value_and_restore_repeatedly() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary dir.");
    let mut store = LocalStorage::new(temp_dir.path().to_path_buf())
        .await
        .expect("unable to create LocalStorage object.");

    store.set(b"key1".to_vec(), b"value1".to_vec()).await?;

    assert_eq!(store.get(b"key1".to_vec()).await?, Some(b"value1".to_vec()));

    store.set(b"key1".to_vec(), b"value2".to_vec()).await?;

    assert_eq!(store.get(b"key1".to_vec()).await?, Some(b"value2".to_vec()));

    drop(store);

    let store = LocalStorage::new(temp_dir.path().to_path_buf())
        .await
        .expect("unable to create LocalStorage object.");
    assert_eq!(store.get(b"key1".to_vec()).await?, Some(b"value2".to_vec()));

    drop(store);

    let store = LocalStorage::new(temp_dir.path().to_path_buf())
        .await
        .expect("unable to create LocalStorage object.");
    assert_eq!(store.get(b"key1".to_vec()).await?, Some(b"value2".to_vec()));

    drop(store);

    let store = LocalStorage::new(temp_dir.path().to_path_buf())
        .await
        .expect("unable to create LocalStorage object.");
    assert_eq!(store.get(b"key1".to_vec()).await?, Some(b"value2".to_vec()));
    Ok(())
}

#[async_std::test]
async fn massive_set() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary dir.");
    info!("DB path: {}", temp_dir.path().to_string_lossy());
    let count = 10000;
    let mut store = LocalStorage::new(temp_dir.path().to_path_buf())
        .await
        .expect("unable to create LocalStorage object.");
    for i in 0..count {
        store
            .set(
                format!("key{}", i).as_bytes().to_vec(),
                format!("value{}", i).as_bytes().to_vec(),
            )
            .await?;
    }
    for i in 0..count {
        let k = format!("key{}", i);
        let val = store.get(k.as_bytes().to_vec()).await?;
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
        .await
        .expect("unable to create LocalStorage object.");
    for i in 0..count {
        let k = format!("key{}", i);
        let val = store.get(k.as_bytes().to_vec()).await?;
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
