use super::result::RaftProposeResult;
use super::rpc::*;
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
}

pub type MsgList = Vec<Message>;
