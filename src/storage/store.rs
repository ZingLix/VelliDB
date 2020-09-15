use super::builder::TableBuilder;
use super::options;
use super::status::LocalStorageStatus;
use super::types::{InternalKey, Key, Value, ValueType};
use super::wal::WalManager;
use crate::{Result, VelliErrorType};
extern crate crossbeam_skiplist;
extern crate fs2;
use crossbeam_skiplist::SkipMap;
use fs2::FileExt;
use std::fs::{self, File};
use std::ops::Bound::Included;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

type MemTable = SkipMap<InternalKey, Option<Value>>;

enum CompactMessage {
    Minor,
    Terminate,
}

pub struct LocalStorage {
    lock_file: File,
    status: Arc<Mutex<LocalStorageStatus>>,
    wal_log: WalManager,
    mmt: MemTable,
    imm: Arc<Mutex<Option<MemTable>>>,
    path: PathBuf,
    compact_sender: mpsc::Sender<CompactMessage>,
    compact_thread: JoinHandle<()>,
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
        let status = LocalStorageStatus::new(&path);
        let mut wal = WalManager::new(&path, status.wal_log_num());
        let (mmt, imm) = Self::recover_sstable(&mut wal)?;
        let status = Arc::new(Mutex::new(status));
        let (sx, rx) = mpsc::channel();

        // verbose below because can not copy object into closure
        // so must clone it manually and then move it into closure
        let imm_copy = Arc::clone(&imm);
        let status_copy = Arc::clone(&status);
        let path_copy = path.clone();
        let compact_thread = thread::spawn(|| {
            Self::compact_background(imm_copy, status_copy, rx, path_copy).ok();
        });

        Ok(LocalStorage {
            lock_file,
            status,
            wal_log: wal,
            mmt,
            imm,
            path,
            compact_sender: sx,
            compact_thread,
        })
    }

    fn recover_sstable(
        wal_manager: &mut WalManager,
    ) -> Result<(MemTable, Arc<Mutex<Option<MemTable>>>)> {
        let mut mmt = SkipMap::new();
        let mut imm = None;
        for item in wal_manager.iterator() {
            // recover memtable
            mmt.insert(item.0, item.1);
            if mmt.len() == options::MAX_MEMTABLE_LENGTH {
                match imm {
                    Some(_) => {}
                    None => {
                        imm = Some(mmt);
                        mmt = SkipMap::new();
                    }
                }
            }
        }
        Ok((mmt, Arc::new(Mutex::new(imm))))
    }

    fn write(&mut self, key: Key, value: Option<Value>) -> Result<()> {
        let value_type = match value {
            Some(_) => ValueType::Value,
            None => ValueType::Deletion,
        };
        let seq_num;
        {
            let mut guard = self.status.lock()?;
            seq_num = (*guard).next_seq_num();
        }
        let ikey = InternalKey::new(key, seq_num, value_type);
        self.wal_log.write(&ikey, &value)?;
        self.mmt.insert(ikey, value);
        if self.mmt.len() >= options::MAX_MEMTABLE_LENGTH {
            self.swap_mmt_imm()?;
        }
        Ok(())
    }

    pub fn set(&mut self, key: Key, value: Vec<u8>) -> Result<()> {
        self.write(key, Some(value))?;
        Ok(())
    }

    fn swap_mmt_imm(&mut self) -> Result<()> {
        match self.imm.try_lock() {
            Ok(mut guard) => match *guard {
                Some(_) => {
                    return Ok(());
                }
                None => {
                    info!("mmt and imm swaped.");
                    let mmt = std::mem::replace(&mut self.mmt, SkipMap::new());
                    *guard = Some(mmt);
                    self.compact_sender.send(CompactMessage::Minor)?;
                }
            },
            Err(_) => {
                return Ok(());
            }
        }
        Ok(())
    }

    pub fn get(&self, key: Key) -> Result<Option<Value>> {
        let value_type = ValueType::Value;
        let seq_num;
        {
            let g = self.status.lock()?;
            seq_num = (*g).cur_seq_num();
        }
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

    fn compact_background(
        imm: Arc<Mutex<Option<MemTable>>>,
        status: Arc<Mutex<LocalStorageStatus>>,
        rx: mpsc::Receiver<CompactMessage>,
        path: PathBuf,
    ) -> Result<()> {
        loop {
            match rx.recv()? {
                CompactMessage::Minor => {
                    info!("Received minor compaction request.");
                    let mut imm_guard = imm.lock()?;
                    if let Some(imm) = imm_guard.take() {
                        drop(imm_guard);
                        let level = 0;
                        let number;
                        {
                            let mut guard = status.lock()?;
                            number = (*guard).next_data_file_num(level);
                        }
                        let mut table_builder = TableBuilder::new(&path, level as u8, number)?;
                        for item in imm.iter() {
                            table_builder.add(item.key(), item.value())?;
                        }
                        let index = table_builder.done()?;
                        {
                            let mut guard = status.lock()?;
                            guard.add_data_file(level, index);
                        }
                    }
                    info!("Minor compaction request finished.");
                }
                CompactMessage::Terminate => {
                    return Ok(());
                }
            }
        }
    }
}

impl Drop for LocalStorage {
    fn drop(&mut self) {
        self.compact_sender.send(CompactMessage::Terminate).ok();
        self.lock_file.unlock().unwrap();
    }
}
