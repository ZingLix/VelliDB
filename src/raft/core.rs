use super::log::LogEntry;
use super::message::{Message, MsgList};
use super::rpc::*;
use rand::prelude::*;
use std::collections::HashMap;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum RaftState {
    Follower,
    Candidate,
    Leader,
}

pub struct NodeCore {
    id: u64,
    state: RaftState,
    node_list: Vec<u64>,
    current_term: u64,
    voted_for: Option<u64>,
    log: Vec<LogEntry>,
    commit_index: usize,
    last_applied: usize,
    next_index: HashMap<u64, usize>,
    match_index: HashMap<u64, usize>,
    vote_count: usize,
    election_elapsed: usize,
    heartbeat_elapsed: usize,
    current_leader_id: Option<u64>,
}

impl NodeCore {
    pub fn new(id: u64, node_list: Vec<u64>) -> NodeCore {
        NodeCore {
            id,
            state: RaftState::Follower,
            node_list,
            current_term: 0,
            voted_for: None,
            log: vec![],
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            vote_count: 0,
            election_elapsed: Self::random_election_timer(),
            heartbeat_elapsed: 0,
            current_leader_id: None,
        }
    }

    pub fn recv_append_entries_rpc(&mut self, request: AppendEntriesRPC) -> AppendEntriesReply {
        trace!(
            "Node {} received AppendEntriesRPC from Node {}.",
            self.id,
            request.leader_id
        );
        // If RPC request or response contains term T > currentTerm:
        // set currentTerm = T, convert to follower (§5.1)
        if request.term > self.current_term {
            self.apply_new_term(request.term, Some(request.leader_id));
            if self.state == RaftState::Candidate {
                self.convert_to_follower();
            }
        }

        let mut reply = AppendEntriesReply {
            term: self.current_term,
            success: false,
        };
        self.heartbeat_elapsed = 0;
        // Reply false if term < currentTerm (§5.1)
        if request.term < self.current_term {
            return reply;
        }
        self.current_leader_id = Some(request.leader_id);
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
                panic!("Invalid log!!!")
            }
        }
        if request.leader_commit > self.commit_index {
            self.commit_index = std::cmp::min(
                request.leader_commit,
                self.log
                    .last()
                    .unwrap_or(&LogEntry {
                        index: 0,
                        term: 0,
                        content: None,
                    })
                    .index,
            );
        }
        reply.success = true;
        reply
    }

    pub fn recv_request_vote_rpc(&mut self, request: RequestVoteRPC) -> RequestVoteReply {
        debug!(
            "Node {} received RequestVoteRPC from Node {}.",
            self.id, request.candidate_id
        );
        // If RPC request or response contains term T > currentTerm:
        // set currentTerm = T, convert to follower (§5.1)
        if request.term > self.current_term {
            self.apply_new_term(request.term, None);
        }

        let mut reply = RequestVoteReply {
            term: self.current_term,
            vote_granted: false,
        };
        // Reply false if term < currentTerm (§5.1)
        if request.term < self.current_term {
            debug!("Node {} refused RequestVoteRPC from node {} on term {} because candidate's term is low.",self.id, request.candidate_id, request.term);
            return reply;
        }
        // If votedFor is null or candidateId, and candidate’s log is at
        // least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
        if self.log.len() == 0
            || request.last_log_term > self.log.last().unwrap().term
            || (request.last_log_term == self.log.last().unwrap().term
                && request.last_log_index >= self.log.last().unwrap().index)
        {
            match self.voted_for {
                None => {
                    debug!(
                        "Node {} granted RequestVoteRPC from node {} on term {}.",
                        self.id, request.candidate_id, request.term
                    );
                    reply.vote_granted = true;
                    self.voted_for = Some(request.candidate_id);
                }
                Some(id) if id == request.candidate_id => {
                    debug!(
                        "Node {} granted RequestVoteRPC from node {} on term {}.",
                        self.id, request.candidate_id, request.term
                    );
                    reply.vote_granted = true;
                }
                Some(id) => {
                    debug!("Node {} refused RequestVoteRPC from node {} on term {} because have voted for node {}.",self.id, request.candidate_id, request.term,id);
                    return reply;
                }
            }
        }
        reply
    }

    pub fn recv_request_vote_reply(
        &mut self,
        id: u64,
        _request: RequestVoteRPC,
        reply: RequestVoteReply,
    ) {
        if reply.term == self.current_term
            && self.state == RaftState::Candidate
            && reply.vote_granted == true
        {
            self.vote_count += 1;
            debug!(
                "Node {} received vote from {}, vote count for term {}: {}",
                self.id, id, self.current_term, self.vote_count,
            );
            if self.vote_count > self.node_list.len() / 2 {
                self.convert_to_leader();
            }
        }
    }

    pub fn recv_append_entries_reply(
        &mut self,
        id: u64,
        request: AppendEntriesRPC,
        reply: AppendEntriesReply,
    ) {
        if self.state != RaftState::Leader {
            return;
        }
        if reply.success {
            if self.log.len() >= self.next_index[&id] && request.entries.len() > 0 {
                self.next_index
                    .insert(id, request.entries.last().unwrap().index + 1);
                self.match_index
                    .insert(id, request.entries.last().unwrap().index);
            }
            let mut match_list = self.match_index.values().cloned().collect::<Vec<usize>>();
            match_list.sort();
            let most = match_list[self.node_list.len() / 2];
            self.commit_index = most;
        } else {
            let index = self.next_index[&id];
            self.next_index
                .insert(id, if index >= 4 { index - 3 } else { 1 });
        }
    }

    pub fn send_request_vote_rpc(&mut self) -> MsgList {
        self.vote_count = 1;
        let mut list = vec![];
        self.voted_for = Some(self.id);
        for node in &self.node_list {
            if node == &self.id {
                continue;
            }
            let mut request = RequestVoteRPC {
                term: self.current_term,
                candidate_id: self.id,
                last_log_index: self
                    .log
                    .last()
                    .unwrap_or(&LogEntry {
                        term: 0,
                        index: 0,
                        content: None,
                    })
                    .index,
                last_log_term: 0,
            };
            if self.log.len() > 0 {
                request.last_log_term = self.log.last().unwrap().term;
            }
            list.push(Message::SendRequestVoteRPC {
                id: node.clone(),
                request,
            });
        }
        list
    }

    pub fn send_append_entries_rpc(&mut self) -> MsgList {
        let mut list = vec![];
        for node in &self.node_list {
            if node == &self.id {
                continue;
            }
            let mut request = AppendEntriesRPC {
                term: self.current_term,
                leader_id: self.id,
                prev_log_index: 0,
                prev_log_term: 0,
                leader_commit: self.commit_index,
                entries: vec![],
            };
            let next_index = self.next_index[&node];
            if self.log.len() > 0 {
                if next_index >= 1 {
                    request.prev_log_index = next_index - 1;
                }
                if self.log.len() > 1 && next_index > 1 {
                    request.prev_log_term = self.log[request.prev_log_index - 1].term;
                }
                request
                    .entries
                    .extend_from_slice(&self.log[request.prev_log_index..]);
            }
            list.push(Message::SendAppendEntriesRPC {
                id: node.clone(),
                request,
            })
        }
        list
    }

    fn next_log_index(&self) -> usize {
        match self.log.last() {
            Some(log) => log.index + 1,
            None => 1,
        }
    }

    pub fn append_log(&mut self, content: Vec<u8>) -> LogEntry {
        let entry = LogEntry {
            index: self.next_log_index(),
            term: self.current_term,
            content: Some(content),
        };
        self.log.push(entry.clone());
        debug!(
            "Node {}: log {}:{} appended.",
            self.id, entry.index, entry.term
        );
        entry
    }

    fn apply_new_term(&mut self, new_term: u64, leader_id: Option<u64>) {
        match leader_id {
            Some(id) => info!(
                "Node {} comes to term {} which leader is {}.",
                self.id, new_term, id
            ),
            None => info!(
                "Node {} comes to term {} with no leader.",
                self.id, new_term
            ),
        }

        self.current_term = new_term;
        self.current_leader_id = leader_id;
        self.voted_for = None;
        self.convert_to_follower();
    }

    fn convert_to_follower(&mut self) {
        if self.state != RaftState::Follower {
            info!("Node {} converts to follower.", self.id);
        }
        self.state = RaftState::Follower;
        self.heartbeat_elapsed = 0;
    }

    fn convert_to_candidate(&mut self) -> MsgList {
        info!("Node {} converts to candidate.", self.id);
        if self.state == RaftState::Candidate {
            self.election_elapsed = Self::random_election_timer();
        }
        self.state = RaftState::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.heartbeat_elapsed = 0;
        self.send_request_vote_rpc()
    }

    fn init_leader_state(&mut self) {
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

    fn convert_to_leader(&mut self) {
        info!("Node {} converts to leader.", self.id);
        self.state = RaftState::Leader;
        self.current_leader_id = Some(self.id);
        self.init_leader_state();
        self.send_append_entries_rpc();
    }

    pub fn tick(&mut self) -> MsgList {
        let mut list = match self.state {
            RaftState::Follower | RaftState::Candidate => self.tick_election(),
            RaftState::Leader => self.send_append_entries_rpc(),
        };
        list.append(&mut self.commit());
        list
    }

    fn random_election_timer() -> usize {
        rand::thread_rng().gen_range(2, 6)
    }

    fn tick_election(&mut self) -> MsgList {
        self.heartbeat_elapsed += 1;
        if self.heartbeat_elapsed > self.election_elapsed {
            return self.convert_to_candidate();
        }
        vec![]
    }

    fn commit(&mut self) -> MsgList {
        let mut msg_list = vec![];
        debug!(
            "Node {}: commit_index {}, last_applied {}",
            self.id, self.commit_index, self.last_applied
        );
        while self.commit_index > self.last_applied {
            msg_list.push(Message::CommitLog {
                log: self.log[self.last_applied].clone(),
            });
            info!("Node {}: log {} applied.", self.id, self.last_applied);
            self.last_applied += 1;
        }
        msg_list
    }

    pub fn state(&self) -> RaftState {
        self.state.clone()
    }

    pub fn terms(&self) -> u64 {
        self.current_term
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn leader_id(&self) -> Option<u64> {
        self.current_leader_id
    }

    pub fn node_count(&self) -> usize {
        self.node_list.len()
    }
}
