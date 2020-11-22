use super::log::LogEntry;

pub struct AppendEntriesRPC {
    pub term: u64,
    pub leader_id: u64,
    pub prev_log_index: usize,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: usize,
}

pub struct AppendEntriesReply {
    pub term: u64,
    pub success: bool,
}

pub struct RequestVoteRPC {
    pub term: u64,
    pub candidate_id: u64,
    pub last_log_index: usize,
    pub last_log_term: u64,
}

pub struct RequestVoteReply {
    pub term: u64,
    pub vote_granted: bool,
}
