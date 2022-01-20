use std::string;

use crate::raft::NodeInfo;

pub struct VelliDBConfig {
    pub server_addr: String,
    pub server_port: u32,
    pub raft_node_list: Vec<NodeInfo>,
    pub raft_node_id: u64,
}
