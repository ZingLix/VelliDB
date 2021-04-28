use super::{
    core::{NodeCore, RaftState},
    message::{Message, MsgList},
    options,
    result::RaftProposeResult,
    rpc::*,
};
use crate::Result;
use crate::{storage::LocalStorage, VelliErrorType};
use async_std::prelude::*;
use async_std::stream;
use async_std::sync::{Arc, Mutex};
use async_std::task;
use async_std::{
    channel::{bounded, unbounded, Receiver, Sender},
    task::block_on,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;
use surf;
use tide::{Request, Response, StatusCode};

#[derive(Clone)]
pub struct NodeInfo {
    pub id: u64,
    pub address: String,
}

impl NodeInfo {
    pub fn new(id: u64, address: String) -> NodeInfo {
        NodeInfo { id, address }
    }
}

pub struct RaftNodeImpl {
    self_info: NodeInfo,
    other_nodes: HashMap<u64, String>,
    #[allow(dead_code)]
    storage: Arc<Mutex<LocalStorage>>,
    server: Option<tide::Server<Arc<Mutex<NodeCore>>>>,
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

async fn recv_propose_request(mut req: Request<Arc<Mutex<NodeCore>>>) -> tide::Result {
    let request: ProposeRequest = req.body_json().await?;
    let node = Arc::clone(req.state());
    let mut guard = node.lock().await;
    let entry = guard.append_log(request.content);
    let reply = ProposeReply { index: Some(entry) };
    let mut response = Response::new(StatusCode::Ok);
    response.set_body(serde_json::to_string(&reply).unwrap());
    Ok(response)
}

impl RaftNodeImpl {
    pub fn new(path: PathBuf, self_info: NodeInfo, other_nodes: Vec<NodeInfo>) -> RaftNodeImpl {
        let node = NodeCore::new(self_info.id, other_nodes.iter().map(|x| x.id).collect());
        let node = Arc::new(Mutex::new(node));
        let (sx, rx) = unbounded();
        RaftNodeImpl {
            self_info,
            other_nodes: other_nodes
                .iter()
                .map(|x| (x.id, x.address.clone()))
                .collect(),
            storage: Arc::new(Mutex::new(block_on(LocalStorage::new(path)).unwrap())),
            server: Some(tide::with_state(node.clone())),
            node,
            msg_list_queue_rx: rx,
            msg_list_queue_sx: sx,
        }
    }

    fn init_server(&mut self) -> Result<()> {
        match self.server.as_mut() {
            Some(server) => {
                server
                    .at(&options::RAFT_REQUEST_VOTE_URI)
                    .post(recv_request_vote_rpc);
                server
                    .at(&options::RAFT_APPEND_ENTRIES_URI)
                    .post(recv_append_entries_rpc);
                server
                    .at(&options::RAFT_PROPOSE_URI)
                    .post(recv_propose_request);
            }
            None => unreachable!(),
        }

        Ok(())
    }

    pub async fn start(mut self) -> Result<()> {
        self.init_server()?;

        task::spawn(
            self.server
                .take()
                .unwrap()
                .listen(self.self_info.address.clone()),
        );

        info!(
            "Node {} running on {}...",
            self.self_info.id, self.self_info.address
        );
        let node = self.node.clone();
        let sx = self.msg_list_queue_sx.clone();
        task::spawn(async move {
            let tick_timeout = Duration::from_millis(300);
            let mut interval = stream::interval(tick_timeout);

            loop {
                while let Some(_) = interval.next().await {
                    let mut guard = node.lock().await;
                    let msg_list = guard.tick();
                    sx.send(msg_list).await.unwrap();
                    //info!("tick");
                }
            }
        });
        self.queue().await?;
        Ok(())
    }

    async fn send_append_entries_rpc(
        &self,
        target_id: u64,
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
        self.msg_list_queue_sx
            .send(vec![Message::RecvAppendEntriesReply {
                id: target_id,
                request,
                reply,
            }])
            .await?;

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
        self.msg_list_queue_sx
            .send(vec![Message::RecvRequestVoteReply {
                id: target_id,
                request,
                reply,
            }])
            .await?;

        Ok(())
    }

    async fn send_propose_request(
        leader_addr: String,
        request: ProposeRequest,
    ) -> Result<ProposeReply> {
        let body = surf::Body::from_json(&request)?;
        let mut response = surf::post(format!(
            "http://{}{}",
            leader_addr,
            options::RAFT_PROPOSE_URI
        ))
        .body(body)
        .await?;
        if response.status() != 200 {
            Err(VelliErrorType::ConnectionError)?
        }
        let reply: ProposeReply = response.body_json().await?;
        info!(
            "recv propose reply: {}",
            serde_json::to_string(&reply).unwrap()
        );
        Ok(reply)
    }

    async fn queue(&mut self) -> Result<()> {
        loop {
            match self.msg_list_queue_rx.recv().await {
                Ok(msg_list) => {
                    for msg in msg_list {
                        match self.deal_msg(msg).await {
                            Ok(()) => {}
                            Err(e) => warn!("Error {} encoutered when dealing message.", e),
                        };
                    }
                }
                Err(_) => {
                    warn!("Node {} error: rx exited.", self.self_info.id);
                    return Err(VelliErrorType::RecvError)?;
                }
            }
        }
    }

    async fn deal_msg(&mut self, msg: Message) -> Result<()> {
        match msg {
            Message::SendAppendEntriesRPC { id, request } => {
                Ok(self.send_append_entries_rpc(id, request).await?)
            }
            Message::SendRequestVoteRPC { id, request } => {
                Ok(self.send_request_vote_rpc(id, request).await?)
            }
            Message::RecvAppendEntriesReply { id, request, reply } => {
                let mut guard = self.node.lock().await;
                Ok(guard.recv_append_entries_reply(id, request, reply))
            }
            Message::RecvRequestVoteReply { id, request, reply } => {
                let mut guard = self.node.lock().await;
                Ok(guard.recv_request_vote_reply(id, request, reply))
            }
            Message::ProposeRequest {
                content,
                mut callback,
            } => {
                let mut g = self.node.lock().await;
                let leader_id = g.leader_id();
                match leader_id {
                    Some(id) => {
                        if id == g.id() {
                            let entry = g.append_log(content);
                            drop(g);
                            callback(RaftProposeResult::Success(entry));
                        } else {
                            drop(g);
                            let leader_addr = self.other_nodes[&id].clone();
                            task::spawn(async move {
                                let reply = Self::send_propose_request(
                                    leader_addr,
                                    ProposeRequest { content },
                                )
                                .await;
                                match reply {
                                    Ok(reply) => match reply.index {
                                        Some(l) => callback(RaftProposeResult::Success(l)),
                                        None => callback(RaftProposeResult::CurrentNoLeader),
                                    },
                                    Err(e) => {
                                        warn!("{}", e);
                                    }
                                }
                            });
                        }
                    }
                    None => {
                        drop(g);
                        callback(RaftProposeResult::CurrentNoLeader);
                    }
                }
                Ok(())
            }
            Message::UpdateNodeInfo { node_info_list } => {
                self.other_nodes = node_info_list
                    .into_iter()
                    .map(|n| (n.id, n.address))
                    .collect();
                Ok(())
            }
        }
    }

    pub(crate) fn sender(&self) -> Sender<MsgList> {
        self.msg_list_queue_sx.clone()
    }

    pub(crate) fn core(&self) -> Arc<Mutex<NodeCore>> {
        Arc::clone(&self.node)
    }

    pub async fn state(&self) -> RaftState {
        let guard = self.node.lock().await;
        guard.state()
    }
}

#[derive(Clone)]
pub struct RaftNodeHandle {
    sx: Sender<MsgList>,
    core: Arc<Mutex<NodeCore>>,
}

impl RaftNodeHandle {
    pub fn new(node: &RaftNodeImpl) -> RaftNodeHandle {
        RaftNodeHandle {
            sx: node.sender(),
            core: node.core(),
        }
    }

    pub async fn propose(&self, message: Vec<u8>) -> Result<RaftProposeResult> {
        let (sx, rx) = bounded(1);
        self.sx
            .send(vec![Message::ProposeRequest {
                content: message,
                callback: Box::new(move |result| match block_on(sx.send(result)) {
                    Ok(()) => (),
                    Err(e) => {
                        warn!("{}", e);
                    }
                }),
            }])
            .await?;
        let result = rx.recv().await?;
        Ok(result)
    }

    pub async fn state(&self) -> RaftState {
        let guard = self.core.lock().await;
        guard.state()
    }

    pub async fn terms(&self) -> u64 {
        let guard = self.core.lock().await;
        guard.terms()
    }

    pub async fn id(&self) -> u64 {
        let guard = self.core.lock().await;
        guard.id()
    }

    pub async fn set_node_info_list(&self, node_info_list: Vec<NodeInfo>) -> Result<()> {
        self.sx
            .send(vec![Message::UpdateNodeInfo { node_info_list }])
            .await?;
        Ok(())
    }

    pub async fn node_count(&self) -> usize {
        let guard = self.core.lock().await;
        guard.node_count()
    }
}
