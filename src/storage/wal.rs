use super::types::{InternalKey, KvPair, Value, ValueType};
use crate::{Result, VelliErrorType};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

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
        let log_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file_path)
            .unwrap();
        WalManager {
            path,
            log_num,
            log_file,
        }
    }

    fn log_filename(log_num: u64) -> String {
        format!("WAL_LOG_{}", log_num)
    }

    #[allow(dead_code)]
    pub fn next(&mut self) -> Result<()> {
        self.log_num += 1;
        let mut path = self.path.clone();
        path.push(Self::log_filename(self.log_num));
        self.log_file = File::create(path)?;
        Ok(())
    }

    pub fn write(&mut self, key: &InternalKey, value: &Option<Value>) -> Result<()> {
        let key_len = key.user_key().len() as u64;
        let mut target_str = Vec::<u8>::new();
        // key length
        target_str.append(&mut key_len.to_be_bytes().to_vec());
        // key
        target_str.append(&mut key.user_key().clone());
        // sequence number
        target_str.append(&mut key.sequence_num().to_be_bytes().to_vec());
        // value type
        match key.value_type() {
            ValueType::Deletion => {
                target_str.push(0u8);
            }
            ValueType::Value => {
                target_str.push(1u8);
                if let Some(v) = value {
                    // value length
                    let value_len = v.len() as u64;
                    target_str.append(&mut value_len.to_be_bytes().to_vec());
                    // value
                    target_str.append(&mut v.clone());
                } else {
                    Err(VelliErrorType::InvalidArguments)?;
                }
            }
        };
        self.log_file.write_all(&target_str)?;
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
        log_file.seek(SeekFrom::Start(0)).unwrap();
        WalIterator { log_file }
    }
}

impl Iterator for WalIterator {
    type Item = KvPair;

    fn next(&mut self) -> Option<Self::Item> {
        let mut buf: [u8; 8] = [0u8; 8];
        let size = self.log_file.read(&mut buf).unwrap();
        if size == 0 {
            // End of file
            return None;
        }
        if size != buf.len() {
            error!("Decode WAL log failed.");
            std::process::exit(1);
        }
        // user key length
        let key_len = u64::from_be_bytes(buf.clone());
        let mut user_key = vec![0; key_len as usize];
        // read user key
        self.log_file.read_exact(&mut user_key).unwrap();
        // read sequence number
        self.log_file.read_exact(&mut buf).unwrap();
        let sequence_num = u64::from_be_bytes(buf.clone());
        let mut value_type_buf: [u8; 1] = [0; 1];
        // read value type
        self.log_file.read(&mut value_type_buf).unwrap();
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
        // read value length
        self.log_file.read_exact(&mut buf).unwrap();
        let value_len = u64::from_be_bytes(buf);
        let mut value = vec![0; value_len as usize];
        // read value
        self.log_file.read_exact(&mut value).unwrap();
        Some((key, Some(value)))
    }
}
