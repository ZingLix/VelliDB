#[macro_use]
extern crate log;
pub mod util;
pub use util::error::{Result, VelliError, VelliErrorType};
pub mod storage;
