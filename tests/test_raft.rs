extern crate log;

use async_std::task;
use std::time::Duration;
use tempfile::TempDir;
use velli_db::{
    raft::{NodeInfo, RaftNodeHandle, RaftNodeImpl, RaftState},
    Result,
};

type NodeVec = Vec<RaftNodeHandle>;

fn check_leader_count(node_list: &NodeVec) -> u64 {
    let mut count = 0;
    for node in node_list {
        let state = task::block_on(node.state()).unwrap();
        if state == RaftState::Leader {
            count += 1;
        }
    }
    count
}

#[async_std::test]
async fn basic() -> Result<()> {
    let mut node_info_list = vec![];
    for i in 1..4 {
        node_info_list.push(NodeInfo::new(i, format!("127.0.0.1:{}", 48880 + i).into()));
    }

    let mut node_list = vec![];
    for i in 1..4 {
        let temp_dir = TempDir::new().expect("unable to create temporary dir.");

        let node = RaftNodeImpl::new(
            temp_dir.path().to_path_buf(),
            NodeInfo::new(i, format!("127.0.0.1:{}", 48880 + i).into()),
            node_info_list.clone(),
        );
        node_list.push(RaftNodeHandle::new(&node));
        task::spawn(async move {
            node.start().await.unwrap();
        });
    }
    let mut leader_count = 0;
    for _ in 0..5 {
        task::sleep(Duration::from_secs(1)).await;
        leader_count = check_leader_count(&node_list);
        if leader_count == 1 {
            return Ok(());
        }
    }
    panic!("Found {} leader(s), expect 1.", leader_count);
}
