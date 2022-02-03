use serde::{Deserialize, Serialize};

use crate::raft::NodeInfo;

#[derive(Serialize, Deserialize)]
pub struct ServerConfig {
    pub address: String,
    pub store_path: String,
}

#[derive(Serialize, Deserialize)]
pub struct VelliDBConfig {
    pub server: ServerConfig,
    pub raft_node: Vec<NodeInfo>,
    pub raft_node_id: Option<u64>,
}
