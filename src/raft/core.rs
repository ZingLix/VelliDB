use super::log::LogEntry;
use super::rpc::*;
use std::collections::HashMap;

#[derive(Debug, PartialEq)]
enum State {
    Follower,
    Candidate,
    Leader,
}

pub struct NodeCore {
    id: u64,
    state: State,
    node_list: Vec<u64>,
    current_term: u64,
    voted_for: Option<u64>,
    log: Vec<LogEntry>,
    commit_index: usize,
    last_applied: u64,
    next_index: HashMap<u64, usize>,
    match_index: HashMap<u64, usize>,
    vote_count: usize,
}

impl NodeCore {
    pub fn new(id: u64, node_list: Vec<u64>) -> NodeCore {
        NodeCore {
            id,
            state: State::Follower,
            node_list,
            current_term: 0,
            voted_for: None,
            log: vec![],
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            vote_count: 0,
        }
    }

    pub fn recvAppendEntriesRPC(&mut self, request: AppendEntriesRPC) -> AppendEntriesReply {
        // If RPC request or response contains term T > currentTerm:
        // set currentTerm = T, convert to follower (§5.1)
        if request.term > self.current_term {
            self.applyNewTerm(request.term);
        }
        if self.state == State::Candidate {
            self.convertToFollower();
        }
        let mut reply = AppendEntriesReply {
            term: self.current_term,
            success: false,
        };
        // Reply false if term < currentTerm (§5.1)
        if request.term < self.current_term {
            return reply;
        }
        // Reply false if log doesn’t contain an entry at prevLogIndex
        // whose term matches prevLogTerm (§5.3)
        if self.log.len() < request.prev_log_index
            || (request.prev_log_index != 0
                && self.log.last().unwrap().term != request.prev_log_term)
        {
            return reply;
        }
        // If an existing entry conflicts with a new one (same index
        // but different terms), delete the existing entry and all that
        // follow it (§5.3)
        if self.log.len() > request.prev_log_index && request.prev_log_index > 0 {
            let log = &self.log[request.prev_log_index - 1];
            if log.term != request.prev_log_term {
                self.log.split_at(request.prev_log_index - 1);
            }
        }
        //  Append any new entries not already in the log
        for entry in request.entries {
            if self.log.len() == entry.index - 1 {
                self.log.push(entry);
            } else {
                panic!("")
            }
        }
        if request.leader_commit > self.commit_index {
            self.commit_index =
                std::cmp::min(request.leader_commit, self.log.last().unwrap().index);
        }
        reply.success = true;
        reply
    }

    pub fn recvRequestVoteRPC(&mut self, request: RequestVoteRPC) -> RequestVoteReply {
        // If RPC request or response contains term T > currentTerm:
        // set currentTerm = T, convert to follower (§5.1)
        if request.term > self.current_term {
            self.applyNewTerm(request.term);
        }

        let mut reply = RequestVoteReply {
            term: self.current_term,
            vote_granted: false,
        };
        // Reply false if term < currentTerm (§5.1)
        if request.term < self.current_term {
            return reply;
        }
        // If votedFor is null or candidateId, and candidate’s log is at
        // least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
        let last_log = self.log.last().unwrap();
        if self.log.len() == 0
            || request.last_log_term > last_log.term
            || (request.last_log_term == last_log.term && request.last_log_index >= last_log.index)
        {
            match self.voted_for {
                None => {
                    reply.vote_granted = true;
                    self.voted_for = Some(request.candidate_id);
                }
                Some(id) if id == request.candidate_id => {
                    reply.vote_granted = true;
                }
                Some(_) => {
                    return reply;
                }
            }
        } else {
            return reply;
        }
        reply
    }

    pub fn recvRequestVoteReply(&mut self, reply: RequestVoteReply) {
        if reply.term == self.current_term
            && self.state == State::Leader
            && reply.vote_granted == true
        {
            self.vote_count += 1;
            if self.vote_count > self.node_list.len() / 2 {
                self.convertToLeader();
            }
        }
    }

    pub fn recvAppendEntriesReply(&mut self, id: u64, last_idx: usize, reply: AppendEntriesReply) {
        if self.state != State::Leader {
            return;
        }
        if reply.success {
            if last_idx >= self.next_index[&id] {
                self.next_index.insert(id, last_idx + 1);
                self.match_index.insert(id, last_idx);
            }
            let mut match_list = self.match_index.values().cloned().collect::<Vec<usize>>();
            match_list.sort();
            let most = match_list[self.node_list.len() / 2 - 1];
            while self.commit_index <= most {
                // commit (self.commit_index)
                self.commit_index += 1;
            }
        } else {
            self.next_index
                .insert(id, std::cmp::max(self.next_index[&id] - 3, 1));
        }
    }

    //TODO
    pub fn sendRequestVoteRPC(&mut self) {
        self.vote_count = 1;
    }
    //TODO
    pub fn sendAppendEntriesRPC(&mut self) {}

    fn applyNewTerm(&mut self, new_term: u64) {
        self.current_term = new_term;
        self.voted_for = None;
        self.convertToFollower();
    }

    fn convertToFollower(&mut self) {
        self.state = State::Follower;
    }

    fn convertToCandidate(&mut self) {
        self.state = State::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.sendRequestVoteRPC();
    }

    fn initLeaderState(&mut self) {
        self.next_index = HashMap::new();
        self.match_index = HashMap::new();
        let last_index = match self.log.last() {
            Some(l) => l.index + 1,
            None => 1,
        };
        for id in &self.node_list {
            self.next_index.insert(id.clone(), last_index);
            self.match_index.insert(id.clone(), 0);
        }
    }

    fn convertToLeader(&mut self) {
        self.state = State::Leader;
        self.initLeaderState();
        self.sendAppendEntriesRPC();
    }
}
