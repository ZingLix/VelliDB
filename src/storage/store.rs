use super::status::LocalStorageStatus;
use super::types::{InternalKey, Key, Value, ValueType};
use super::wal::{WalIterator, WalManager};
use crate::{Result, VelliErrorType};
extern crate crossbeam_skiplist;
extern crate fs2;
use crossbeam_skiplist::SkipMap;
use fs2::FileExt;
use std::fs::{self, File};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

type MemTable = Arc<SkipMap<InternalKey, Option<Value>>>;

pub struct LocalStorage {
    lock_file: File,
    status: LocalStorageStatus,
    wal_log: WalManager,
    mmt: MemTable,
    imm: MemTable,
}

impl LocalStorage {
    pub fn new(path: PathBuf) -> Result<LocalStorage> {
        let lock_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.join("LOCK"))
            .expect("Open LOCK file failed");

        match lock_file.try_lock_exclusive() {
            Ok(()) => {
                info!("LocalStorage got file lock.");
            }
            Err(_) => {
                error!("LocalStorage can not get the file lock.");
                return Err(VelliErrorType::LockFailed)?;
            }
        }
        let mut status = LocalStorageStatus::new(&path);
        let mut wal = WalManager::new(&path, status.wal_log_num());
        let (mmt, imm) = Self::recover_sstable(&mut wal, &mut status)?;

        Ok(LocalStorage {
            lock_file,
            status,
            wal_log: wal,
            mmt,
            imm,
        })
    }

    fn recover_sstable(
        wal_manager: &mut WalManager,
        status: &mut LocalStorageStatus,
    ) -> Result<(MemTable, MemTable)> {
        let mmt = Arc::new(SkipMap::new());
        let imm = Arc::new(SkipMap::new());
        for item in wal_manager.iterator() {
            (*mmt).insert(item.0, item.1);
            // recover sstable
        }
        status.update_log_num();
        wal_manager.next()?;
        Ok((mmt, imm))
    }

    fn write(&mut self, key: Key, value: Option<Value>) -> Result<()> {
        let value_type = match value {
            Some(_) => ValueType::Value,
            None => ValueType::Deletion,
        };
        let seq_num = self.status.next_seq_num();
        let ikey = InternalKey::new(key, seq_num, value_type);
        self.wal_log.write(&ikey, &value)?;
        self.mmt.insert(ikey, value);
        Ok(())
    }

    pub fn set(&mut self, key: Key, value: Vec<u8>) -> Result<()> {
        self.write(key, Some(value))?;
        Ok(())
    }

    pub fn get(&self, key: Key) -> Result<Option<Value>> {
        let value_type = ValueType::Value;
        let seq_num = self.status.cur_seq_num();
        let ikey = InternalKey::new(key, seq_num, value_type);
        let val = self.mmt.upper_bound(Included(&ikey));
        match val {
            Some(entry) => {
                if entry.key().user_key().eq(ikey.user_key()) {
                    return Ok(entry.value().clone());
                }
                return Ok(None);
            }
            None => Ok(None),
        }
    }

    pub fn delete(&mut self, key: Key) -> Result<()> {
        self.write(key, None)?;
        Ok(())
    }
}

impl Drop for LocalStorage {
    fn drop(&mut self) {
        self.lock_file.unlock().unwrap();
    }
}
