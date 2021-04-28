use super::log::LogEntry;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
pub struct AppendEntriesRPC {
    pub term: u64,
    pub leader_id: u64,
    pub prev_log_index: usize,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: usize,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct AppendEntriesReply {
    pub term: u64,
    pub success: bool,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct RequestVoteRPC {
    pub term: u64,
    pub candidate_id: u64,
    pub last_log_index: usize,
    pub last_log_term: u64,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct RequestVoteReply {
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ProposeRequest {
    pub content: Vec<u8>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ProposeReply {
    pub index: Option<LogEntry>,
}
