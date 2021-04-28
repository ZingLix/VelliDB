use super::log::LogEntry;

pub enum RaftProposeResult {
    Success(LogEntry),
    CurrentNoLeader,
}
