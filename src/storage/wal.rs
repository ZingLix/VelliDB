use super::types::{InternalKey, KvPair, ValueType};
use crate::{Result, VelliErrorType};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub struct WalManager {
    path: PathBuf,
    log_num: u64,
    log_file: File,
}

impl WalManager {
    pub fn new(path: &PathBuf, log_num: u64) -> WalManager {
        let mut path = path.clone();
        path.push("wal");
        if log_num == 0 {
            // first open, wal folder does not exist
            fs::create_dir_all(&path).unwrap();
        }
        let mut file_path = path.clone();
        file_path.push(Self::log_filename(log_num));
        let log_file = File::open(file_path).unwrap();
        WalManager {
            path,
            log_num,
            log_file,
        }
    }

    fn log_filename(log_num: u64) -> String {
        format!("WAL_LOG_{}", log_num)
    }

    pub fn next(&mut self) -> Result<()> {
        self.log_num += 1;
        let mut path = self.path.clone();
        path.push(Self::log_filename(self.log_num));
        self.log_file = File::open(path)?;
        Ok(())
    }

    pub fn iterator(&self) -> WalIterator {
        WalIterator::new(&self.log_file)
    }
}

pub struct WalIterator {
    log_file: File,
}

impl WalIterator {
    pub fn new(log_file: &File) -> WalIterator {
        let mut log_file = log_file.try_clone().unwrap();
        log_file.seek(SeekFrom::Start(0));
        WalIterator { log_file }
    }
}

impl Iterator for WalIterator {
    type Item = KvPair;

    fn next(&mut self) -> Option<Self::Item> {
        let mut buf: [u8; 8] = [0u8; 8];
        let size = self.log_file.read(&mut buf).unwrap();
        if size == 0 {
            return None;
        }
        if size != buf.len() {
            error!("Decode WAL log failed.");
            std::process::exit(1);
        }
        let key_len = u64::from_be_bytes(buf.clone());
        let mut user_key = vec![0; key_len as usize];
        self.log_file.read_exact(&mut user_key);
        self.log_file.read_exact(&mut buf);
        let sequence_num = u64::from_be_bytes(buf.clone());
        let mut value_type_buf: [u8; 1] = [0; 1];
        self.log_file.read(&mut value_type_buf);
        let value_type = match value_type_buf[0] {
            0 => ValueType::Deletion,
            1 => ValueType::Value,
            _ => {
                error!("Decode WAL log failed.");
                std::process::exit(1);
            }
        };
        let key = InternalKey::new(user_key, sequence_num, value_type);
        if value_type == ValueType::Deletion {
            return Some((key, None));
        }
        self.log_file.read_exact(&mut buf);
        let value_len = u64::from_be_bytes(buf);
        let mut value = vec![0; value_len as usize];
        self.log_file.read_exact(&mut value);
        Some((key, Some(value)))
    }
}
