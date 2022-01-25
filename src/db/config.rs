use serde::{Deserialize, Serialize};

use crate::raft::NodeInfo;

#[derive(Serialize, Deserialize)]
struct ServerConfig {
    address: String,
    port: u32,
}

#[derive(Serialize, Deserialize)]
pub struct VelliDBConfig {
    pub server_addr: String,
    pub server_port: u32,
    pub raft_node_list: Vec<NodeInfo>,
    pub raft_node_id: u64,
}
