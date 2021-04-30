use super::{log::LogEntry, rpc::*};
use super::{result::RaftProposeResult, NodeInfo};
use async_std::channel::Sender;

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
    PersistCurrentTerm {
        term: u64,
    },
    PersistVotedFor {
        voted_for: Option<u64>,
    },
    PersistLog {
        log: Vec<LogEntry>,
    },
    CommitLog {
        log: LogEntry,
    },
    Callback {
        sx: Sender<()>,
    },
}

pub type MsgList = Vec<Message>;
