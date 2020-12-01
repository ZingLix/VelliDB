use super::core::{MsgList, NodeCore};
use super::message::Message;
use super::options;
use super::rpc::*;
use crate::Result;
use crate::{storage::LocalStorage, VelliErrorType};
use async_std::prelude::*;
use async_std::stream::{self, Interval};
use async_std::sync::{Arc, Mutex};
use async_std::task;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::time::{Duration, Instant};
use surf;
use tide::{Request, Response, StatusCode};

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
    msg_list_queue_rx: Receiver<MsgList>,
    msg_list_queue_sx: Sender<MsgList>,
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
        let (sx, rx) = channel();
        Node {
            self_info,
            other_nodes: other_nodes
                .iter()
                .map(|x| (x.id, x.address.clone()))
                .collect(),
            storage: LocalStorage::new(path).unwrap(),
            server: tide::with_state(node.clone()),
            node,
            msg_list_queue_rx: rx,
            msg_list_queue_sx: sx,
        }
    }

    fn init_server(&mut self) -> Result<()> {
        self.server
            .at(&options::RAFT_REQUEST_VOTE_URI)
            .post(recv_request_vote_rpc);
        self.server
            .at(&options::RAFT_APPEND_ENTRIES_URI)
            .post(recv_append_entries_rpc);
        Ok(())
    }

    pub async fn start(mut self) -> Result<()> {
        self.init_server()?;
        self.server.listen(self.self_info.address.clone()).await?;
        let tick_timeout = Duration::from_millis(100);
        let mut interval = stream::interval(tick_timeout);

        loop {
            while let Some(_) = interval.next().await {
                let mut guard = self.node.lock().await;
                guard.tick();
            }
        }

        Ok(())
    }

    async fn send_append_entries_rpc(
        &self,
        target_id: u64,
        last_idx: usize,
        request: AppendEntriesRPC,
    ) -> Result<()> {
        let body = surf::Body::from_json(&request)?;
        let mut response = surf::post(format!(
            "http://{}{}",
            self.other_nodes[&target_id],
            options::RAFT_APPEND_ENTRIES_URI
        ))
        .body(body)
        .await?;
        if response.status() != 200 {
            Err(VelliErrorType::ConnectionError)?
        }
        let reply: AppendEntriesReply = response.body_json().await?;
        let mut guard = self.node.lock().await;
        guard.recv_append_entries_reply(target_id, last_idx, reply);
        Ok(())
    }

    async fn send_request_vote_rpc(&self, target_id: u64, request: RequestVoteRPC) -> Result<()> {
        let body = surf::Body::from_json(&request)?;
        let mut response = surf::post(format!(
            "http://{}{}",
            self.other_nodes[&target_id],
            options::RAFT_REQUEST_VOTE_URI
        ))
        .body(body)
        .await?;
        if response.status() != 200 {
            Err(VelliErrorType::ConnectionError)?
        }
        let reply: RequestVoteReply = response.body_json().await?;

        let mut guard = self.node.lock().await;
        guard.recv_request_vote_reply(reply);
        Ok(())
    }

    async fn queue(&mut self) {}
}
