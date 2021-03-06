use super::{log::LogEntry, rpc::*};
use super::{result::RaftProposeResult, NodeInfo};

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
        callback: Box<dyn FnMut(RaftProposeResult) -> () + Send + Sync>,
    },
    UpdateNodeInfo {
        node_info_list: Vec<NodeInfo>,
    },
    CommitLog {
        log: LogEntry,
    },
}

pub type MsgList = Vec<Message>;
