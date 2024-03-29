use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, Serialize, Hash, PartialEq, Eq, Debug)]
pub struct LogEntry {
    pub term: u64,
    pub index: usize,
    pub content: Option<Vec<u8>>,
}
