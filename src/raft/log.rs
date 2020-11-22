use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, Serialize)]
pub struct LogEntry {
    pub term: u64,
    pub index: usize,
}
