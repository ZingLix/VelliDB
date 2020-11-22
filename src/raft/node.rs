use super::core::NodeCore;
use super::rpc::*;
use crate::storage::LocalStorage;
use crate::Result;
use async_std::sync::{Arc, Mutex};
use async_std::task;
use std::collections::HashMap;
use std::path::PathBuf;
use tide::{Request, Response, StatusCode};

#[derive(Clone)]
enum Event {
    RequsetVoteRPC { request: RequestVoteRPC },
}

pub struct NodeInfo {
    id: u64,
    address: String,
}

impl NodeInfo {
    pub fn new(id: u64, address: String) -> NodeInfo {
        NodeInfo { id, address }
    }
}

pub struct Node {
    self_info: NodeInfo,
    other_nodes: HashMap<u64, String>,
    storage: LocalStorage,
    server: tide::Server<Arc<Mutex<NodeCore>>>,
    node: Arc<Mutex<NodeCore>>,
}

async fn recv_request_vote_rpc(mut req: Request<Arc<Mutex<NodeCore>>>) -> tide::Result {
    let request: RequestVoteRPC = req.body_json().await?;
    let node = Arc::clone(req.state());
    let mut guard = node.lock().await;
    let reply = guard.recv_request_vote_rpc(request);
    let mut response = Response::new(StatusCode::Ok);
    response.set_body(serde_json::to_string(&reply).unwrap());
    Ok(response)
}

async fn recv_append_entries_rpc(mut req: Request<Arc<Mutex<NodeCore>>>) -> tide::Result {
    let request: AppendEntriesRPC = req.body_json().await?;
    let node = Arc::clone(req.state());
    let mut guard = node.lock().await;
    let reply = guard.recv_append_entries_rpc(request);
    let mut response = Response::new(StatusCode::Ok);
    response.set_body(serde_json::to_string(&reply).unwrap());
    Ok(response)
}

impl Node {
    pub fn new(path: PathBuf, self_info: NodeInfo, other_nodes: Vec<NodeInfo>) -> Node {
        let node = NodeCore::new(self_info.id, other_nodes.iter().map(|x| x.id).collect());
        let node = Arc::new(Mutex::new(node));
        Node {
            self_info,
            other_nodes: other_nodes
                .iter()
                .map(|x| (x.id, x.address.clone()))
                .collect(),
            storage: LocalStorage::new(path).unwrap(),
            server: tide::with_state(node.clone()),
            node,
        }
    }

    fn init_server(&mut self) -> Result<()> {
        self.server
            .at("/raft/request_vote")
            .post(recv_request_vote_rpc);
        self.server
            .at("/raft/append_entries")
            .post(recv_append_entries_rpc);
        Ok(())
    }

    pub async fn start(mut self) -> Result<()> {
        self.init_server()?;
        self.server.listen(self.self_info.address.clone()).await?;
        Ok(())
    }
}
