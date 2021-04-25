use super::rpc::*;
use super::{result::RaftProposeResult, NodeInfo};
use crate::Result;

pub enum Message {
    SendRequestVoteRPC {
        id: u64,
        request: RequestVoteRPC,
    },
    SendAppendEntriesRPC {
        id: u64,
        request: AppendEntriesRPC,
    },
    RecvRequestVoteReply {
        id: u64,
        request: RequestVoteRPC,
        reply: RequestVoteReply,
    },
    RecvAppendEntriesReply {
        id: u64,
        request: AppendEntriesRPC,
        reply: AppendEntriesReply,
    },
    ProposeRequest {
        content: Vec<u8>,
        callback: Box<dyn FnMut(RaftProposeResult) -> Result<()> + Send + Sync>,
    },
    UpdateNodeInfo {
        node_info_list: Vec<NodeInfo>,
    },
}

pub type MsgList = Vec<Message>;
