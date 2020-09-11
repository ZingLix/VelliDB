use super::wal::{WalIterator, WalManager};
use super::{status::LocalStorageStatus, InternalKey, Key};
use crate::{Result, VelliErrorType};
extern crate fs2;
use fs2::FileExt;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

struct LocalStorage {
    lock_file: File,
    status: LocalStorageStatus,
    wal_log: WalManager,
}

impl LocalStorage {
    fn new(path: PathBuf) -> Result<LocalStorage> {
        let lock_file = File::open(path.join("LOCK"))?;
        match lock_file.try_lock_exclusive() {
            Ok(()) => {
                info!("LocalStorage got file lock.");
            }
            Err(_) => {
                error!("LocalStorage can not get the file lock.");
                return Err(VelliErrorType::LockFailed)?;
            }
        }
        let status = LocalStorageStatus::new(&path);
        let wal = WalManager::new(&path, status.wal_log_num());
        Ok(LocalStorage {
            lock_file,
            status,
            wal_log: wal,
        })
    }

    fn recover_sstable(wal_manager: &mut WalManager) -> Result<()> {
        for i in wal_manager.iterator() {
            // recover sstable
        }
        Ok(())
    }

    fn set(key: Key, value: Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn get(key: Key) -> Result<Vec<u8>> {
        Ok(vec![])
    }

    fn delete(key: Key) -> Result<()> {
        Ok(())
    }
}

impl Drop for LocalStorage {
    fn drop(&mut self) {
        self.lock_file.unlock().unwrap();
    }
}
