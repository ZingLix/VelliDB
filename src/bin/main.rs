#[macro_use]
extern crate log;

use log::Level;
use log::LevelFilter;
use tempfile::TempDir;
use velli_db::LocalStorage;
fn main() {
    env_logger::builder().filter_level(LevelFilter::Info).init();
    let temp_dir = TempDir::new().expect("unable to create temporary dir.");

    let mut store = LocalStorage::new(temp_dir.path().to_path_buf())
        .expect("unable to create LocalStorage object.");
    for i in 0..10000 {
        store
            .set(
                format!("key{}", i).as_bytes().to_vec(),
                format!("value{}", i).as_bytes().to_vec(),
            )
            .ok();
    }
    drop(store)
}
