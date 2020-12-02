#[macro_use]
extern crate log;
use log::LevelFilter;

use async_std::task;
use tempfile::TempDir;
use velli_db::{raft::NodeInfo, Node, Result};

#[test]
fn basic() -> Result<()> {
    env_logger::builder().filter_level(LevelFilter::Info).init();
    let temp_dir = TempDir::new().expect("unable to create temporary dir.");
    let node = Node::new(
        temp_dir.path().to_path_buf(),
        NodeInfo::new(1, "127.0.0.1:48880".into()),
        vec![],
    );
    task::block_on(node.start())?;
    Ok(())
}
