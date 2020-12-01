use super::rpc::*;

#[derive(Clone)]
pub enum Message {
    SendRequestVoteRPC { id: u64, request: RequestVoteRPC },
    SendAppendEntriesRPC { id: u64, request: AppendEntriesRPC },
    RecvRequestVoteReply { id: u64, rplay: RequestVoteReply },
    RecvAppendEntriesReply { id: u64, reply: AppendEntriesReply },
}