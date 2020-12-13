extern crate log;

use async_std::task;
use tempfile::TempDir;
use velli_db::{raft::NodeInfo, Node, Result};

#[async_std::test]
async fn basic() -> Result<()> {
    env_logger::Builder::new()
        .parse_filters("warn,velli_db=info")
        .init();
    let mut handle = None;
    let mut node_list = vec![];
    for i in 1..4 {
        node_list.push(NodeInfo::new(i, format!("127.0.0.1:{}", 48880 + i).into()));
    }

    for i in 1..4 {
        let temp_dir = TempDir::new().expect("unable to create temporary dir.");

        let node = Node::new(
            temp_dir.path().to_path_buf(),
            NodeInfo::new(i, format!("127.0.0.1:{}", 48880 + i).into()),
            node_list.clone(),
        );
        handle = Some(task::spawn(async move {
            node.start().await.unwrap();
        }));
    }
    handle.unwrap().await;
    Ok(())
}
