#[macro_use]
extern crate log;
pub mod util;
pub use util::error::{Result, VelliError, VelliErrorType};
pub mod storage;
pub use storage::LocalStorage;
pub mod db;
pub use db::{VelliDB, VelliDBConfig};
pub mod raft;
