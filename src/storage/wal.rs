use super::types::{InternalKey, KvPair, Value, ValueType};
use crate::{Result, VelliErrorType};
use std::fs::{self, File};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
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
        target_str.append(&mut key_len.to_le_bytes().to_vec());
        // key as bytes
        target_str.append(&mut key.to_bytes());
        if key.value_type() != ValueType::Deletion {
            if let Some(v) = value {
                // value length
                let value_len = v.len() as u64;
                target_str.append(&mut value_len.to_le_bytes().to_vec());
                // value
                target_str.append(&mut v.clone());
            } else {
                Err(VelliErrorType::InvalidArguments)?;
            }
        }
        self.log_file.write_all(&target_str)?;
        self.log_file.flush()?;
        Ok(())
    }

    pub fn iterator(&self) -> WalIterator {
        WalIterator::new(&self.log_file)
    }
}

pub struct WalIterator {
    log_file: BufReader<File>,
}

impl WalIterator {
    pub fn new(log_file: &File) -> WalIterator {
        let mut log_file = BufReader::new(log_file.try_clone().unwrap());
        log_file.seek(SeekFrom::Start(0)).unwrap();
        WalIterator { log_file }
    }
}

impl Iterator for WalIterator {
    type Item = KvPair;

    fn next(&mut self) -> Option<Self::Item> {
        let mut buf: [u8; 8] = [0u8; 8];
        match self.log_file.read_exact(&mut buf) {
            Ok(size) => size,
            Err(_) => {
                return None;
            }
        };

        // user key length
        let key_len = u64::from_le_bytes(buf.clone());
        let mut key_bytes = vec![0; key_len as usize + InternalKey::len_without_key()];
        // read key
        self.log_file.read_exact(&mut key_bytes).unwrap();
        let key = InternalKey::decode(&key_bytes).expect("Decode WAL log key failed.");

        if key.value_type() == ValueType::Deletion {
            return Some((key, None));
        }
        // read value length
        self.log_file.read_exact(&mut buf).unwrap();
        let value_len = u64::from_le_bytes(buf);
        let mut value = vec![0; value_len as usize];
        // read value
        self.log_file.read_exact(&mut value).unwrap();
        Some((key, Some(value)))
    }
}
