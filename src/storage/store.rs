use super::builder::TableBuilder;
use super::options;
use super::status::LocalStorageStatus;
use super::types::{InternalKey, Key, Value, ValueType};
use super::wal::WalManager;
use crate::{Result, VelliErrorType};
extern crate crossbeam_skiplist;
extern crate fs2;
use async_std::channel::{self, Receiver, Sender};
use async_std::sync::{Arc, Mutex};
use async_std::task::{block_on, spawn, JoinHandle};
use crossbeam_skiplist::SkipMap;
use fs2::FileExt;
use std::fs::{self, File};
use std::ops::Bound::Included;
use std::path::PathBuf;

type MemTable = SkipMap<InternalKey, Option<Value>>;

enum CompactMessage {
    Minor(Arc<Option<MemTable>>, Arc<Mutex<bool>>),
    Terminate,
}

pub struct LocalStorage {
    lock_file: File,
    status: Arc<Mutex<LocalStorageStatus>>,
    wal_log: WalManager,
    mmt: MemTable,
    imm: Arc<Option<MemTable>>,
    #[allow(dead_code)]
    path: PathBuf,
    compact_sender: Sender<CompactMessage>,
    #[allow(dead_code)]
    compact_thread: JoinHandle<Result<()>>,
    minor_compacting: Arc<Mutex<bool>>,
}

impl LocalStorage {
    pub async fn new(path: PathBuf) -> Result<LocalStorage> {
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
        let mut wal = WalManager::new(&path, status.wal_log_num()).await;
        let (mmt, imm) = Self::recover_sstable(&mut wal).await?;
        let status = Arc::new(Mutex::new(status));
        let (sx, rx) = channel::unbounded();

        // verbose below because can not copy object into closure
        // so must clone it manually and then move it into closure
        let status_copy = Arc::clone(&status);
        let path_copy = path.clone();
        let compact_thread = spawn(Self::compact_background(status_copy, rx, path_copy));

        Ok(LocalStorage {
            lock_file,
            status,
            wal_log: wal,
            mmt,
            imm,
            path,
            compact_sender: sx,
            compact_thread,
            minor_compacting: Arc::new(Mutex::new(false)),
        })
    }

    async fn recover_sstable(
        wal_manager: &mut WalManager,
    ) -> Result<(MemTable, Arc<Option<MemTable>>)> {
        let mut mmt = SkipMap::new();
        let mut imm = None;
        for item in wal_manager.iterator().await {
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
        Ok((mmt, Arc::new(imm)))
    }

    async fn write(&mut self, key: Key, value: Option<Value>) -> Result<()> {
        let value_type = match value {
            Some(_) => ValueType::Value,
            None => ValueType::Deletion,
        };
        let seq_num;
        {
            let mut guard = self.status.lock().await;
            seq_num = (*guard).next_seq_num();
        }
        let ikey = InternalKey::new(key, seq_num, value_type);
        self.wal_log.write(&ikey, &value).await?;
        self.mmt.insert(ikey, value);
        if self.mmt.len() >= options::MAX_MEMTABLE_LENGTH {
            self.swap_mmt_imm().await?;
        }
        Ok(())
    }

    pub async fn set(&mut self, key: Key, value: Vec<u8>) -> Result<()> {
        self.write(key, Some(value)).await?;
        Ok(())
    }

    async fn swap_mmt_imm(&mut self) -> Result<()> {
        match self.minor_compacting.try_lock() {
            Some(mut guard) => {
                if *guard == false {
                    let mmt = std::mem::replace(&mut self.mmt, SkipMap::new());
                    self.imm = Arc::new(Some(mmt));
                    *guard = true;
                    self.compact_sender
                        .send(CompactMessage::Minor(
                            Arc::clone(&self.imm),
                            Arc::clone(&self.minor_compacting),
                        ))
                        .await?;
                    info!("mmt and imm swapped.");
                }
            }
            None => return Ok(()),
        }

        Ok(())
    }

    pub async fn get(&self, key: Key) -> Result<Option<Value>> {
        let value_type = ValueType::Value;
        let seq_num;
        {
            let g = self.status.lock().await;
            seq_num = (*g).cur_seq_num();
        }
        let ikey = InternalKey::new(key, seq_num, value_type);
        let val = self.mmt.upper_bound(Included(&ikey));
        match val {
            Some(entry) => {
                if entry.key().user_key().eq(ikey.user_key()) {
                    return Ok(entry.value().clone());
                }
            }
            None => {}
        }
        if let Some(m) = &*self.imm {
            let entry = m.upper_bound(Included(&ikey));
            match entry {
                Some(entry) => {
                    if entry.key().user_key().eq(ikey.user_key()) {
                        return Ok(entry.value().clone());
                    }
                }
                None => {}
            }
        }
        for i in 0..options::MAX_DATA_FILE_LEVEL {
            let mut table_list;
            {
                let guard = self.status.lock().await;
                table_list = guard.possible_table_file(i as u8, &ikey)?;
            }
            for reader in table_list.iter_mut() {
                reader.upper_bound(&ikey)?;
                if let Some(kv) = reader.cur() {
                    if &kv.0.user_key() == &ikey.user_key() {
                        return Ok(kv.1);
                    }
                }
            }
        }
        Ok(None)
    }

    pub async fn delete(&mut self, key: Key) -> Result<()> {
        self.write(key, None).await?;
        Ok(())
    }

    async fn compact_background(
        status: Arc<Mutex<LocalStorageStatus>>,
        rx: Receiver<CompactMessage>,
        path: PathBuf,
    ) -> Result<()> {
        loop {
            match rx.recv().await? {
                CompactMessage::Minor(imm, compacting) => {
                    info!("Received minor compaction request.");
                    let mut compacting_guard = compacting.lock().await;
                    let level = 0;
                    let number;
                    {
                        let mut guard = status.lock().await;
                        number = (*guard).next_data_file_num(level);
                    }
                    let mut table_builder = TableBuilder::new(&path, level as u8, number)?;
                    match &*imm {
                        Some(map) => {
                            for item in map.iter() {
                                table_builder.add(item.key(), item.value())?;
                            }
                        }
                        None => {
                            *compacting_guard = false;
                            continue;
                        }
                    }
                    let index = table_builder.done()?;
                    {
                        let mut guard = status.lock().await;
                        guard.add_data_file(level, index)?;
                    }
                    *compacting_guard = false;
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
        block_on(self.compact_sender.send(CompactMessage::Terminate)).unwrap();
        self.lock_file.unlock().unwrap();
    }
}
