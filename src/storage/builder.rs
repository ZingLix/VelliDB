use super::options;
use super::status::DataLevelIndex;
use super::types::{InternalKey, Value};
use crate::Result;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

struct BlockEntry {
    shared_bytes: u64,
    unshared_bytes: u64,
    value_bytes: u64,
    unshared_key_data: Vec<u8>,
    value_data: Value,
}

impl BlockEntry {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![];
        bytes.append(&mut self.shared_bytes.to_le_bytes().to_vec());
        bytes.append(&mut self.unshared_bytes.to_le_bytes().to_vec());
        bytes.append(&mut self.value_bytes.to_le_bytes().to_vec());
        bytes.append(&mut self.unshared_key_data.clone());
        bytes.append(&mut self.value_data.clone());
        bytes
    }

    pub fn len(&self) -> usize {
        8 + 8 + 8 + self.unshared_key_data.len() + self.value_data.len()
    }
}

#[derive(Default)]
struct BlockBuilder {
    last_key: Vec<u8>,
    entry_list: Vec<BlockEntry>,
    length: u64,
    restart_list: Vec<u64>,
}

impl BlockBuilder {
    fn new() -> Self {
        BlockBuilder {
            last_key: vec![],
            entry_list: vec![],
            length: 0,
            restart_list: vec![],
        }
    }

    fn add(&mut self, key: &InternalKey, value: &Vec<u8>) {
        let key_bytes = key.to_bytes();
        let shared_bytes = Self::compare_bytes(&self.last_key, &key_bytes) as u64;
        let unshared_bytes = key_bytes.len() as u64 - shared_bytes;
        let value_bytes = value.len() as u64;
        let unshared_key_data = key_bytes[shared_bytes as usize..].to_vec();
        let entry = BlockEntry {
            shared_bytes,
            unshared_bytes,
            value_bytes,
            unshared_key_data,
            value_data: value.clone(),
        };
        self.length += entry.len() as u64;
        self.entry_list.push(entry);
        if self.entry_list.len() % options::BLOCK_RESTART_INTERVAL != 0 {
            self.last_key = key_bytes;
        } else {
            self.last_key = vec![];
            self.restart_list.push(self.length);
        }
    }

    fn compare_bytes(lhs: &Vec<u8>, rhs: &Vec<u8>) -> usize {
        let min_len = std::cmp::min(lhs.len(), rhs.len());
        if min_len == 0 {
            return 0;
        }
        let mut count = 0;
        for _ in 0..min_len - 1 {
            if lhs[count] != rhs[count] {
                return count;
            }
            count += 1;
        }
        count
    }

    fn build(mut self) -> Vec<u8> {
        let mut res = vec![];
        if self.restart_list.len() > 0 && self.restart_list.last().unwrap() == &self.length {
            self.restart_list.pop();
        }
        // restart count
        res.append(&mut (self.restart_list.len() as u64).to_le_bytes().to_vec());
        // restart offset
        for item in self.restart_list.iter() {
            res.append(&mut (item + self.prepend_length()).to_le_bytes().to_vec());
        }
        // entry list
        for item in self.entry_list.iter() {
            res.append(&mut item.to_bytes());
        }
        res
    }

    fn prepend_length(&self) -> u64 {
        let restart_len =
            if self.restart_list.len() > 0 && self.restart_list.last().unwrap() == &self.length {
                self.restart_list.len() - 1
            } else {
                self.restart_list.len()
            };
        8 + 8 * restart_len as u64
    }

    fn length(&self) -> u64 {
        self.length + self.prepend_length()
    }

    fn count(&self) -> usize {
        self.entry_list.len()
    }
}

pub struct TableIndex {
    pub offset: u64,
    pub key_len: u64,
    pub last_key: Vec<u8>,
}

impl TableIndex {
    pub fn to_bytes(mut self) -> Vec<u8> {
        let mut res = vec![];
        res.append(&mut self.offset.to_le_bytes().to_vec());
        res.append(&mut self.key_len.to_le_bytes().to_vec());
        res.append(&mut self.last_key);
        res
    }
}

pub struct TableBuilder {
    level: u8,
    number: u64,
    path: PathBuf,
    block_builder: BlockBuilder,
    table_file: File,
    index_list: Vec<TableIndex>,
    start_key: Option<InternalKey>,
    offset: u64,
    last_key: Vec<u8>,
}

pub fn table_file_name_tmp(level: u8, number: u64) -> String {
    format!("data_table_{}_{}.tmp", level, number)
}

pub fn table_file_name(level: u8, number: u64) -> String {
    format!("data_table_{}_{}", level, number)
}

impl TableBuilder {
    pub fn new(base_path: &PathBuf, level: u8, number: u64) -> Result<Self> {
        let mut path = base_path.clone();
        path.push("data");
        std::fs::create_dir_all(&path).unwrap();
        path.push(table_file_name_tmp(level, number));
        let table_file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&path)?;
        path.pop();
        Ok(TableBuilder {
            level,
            number,
            path,
            block_builder: BlockBuilder::new(),
            table_file,
            index_list: vec![],
            start_key: None,
            offset: 0,
            last_key: vec![],
        })
    }

    pub fn add(&mut self, key: &InternalKey, value: &Option<Vec<u8>>) -> Result<()> {
        if String::from_utf8(key.user_key().clone()).unwrap() == "key53" {
            info!("{}", String::from_utf8(key.user_key().clone()).unwrap());
        }
        if self.start_key == None {
            self.start_key = Some(key.clone());
        }
        if let Some(v) = value {
            self.block_builder.add(key, v);
        } else {
            self.block_builder.add(key, &vec![]);
        }
        self.last_key = key.to_bytes();
        if self.block_builder.length() > options::BLOCK_SIZE as u64 {
            self.write_block()?;
        }
        Ok(())
    }

    fn write_block(&mut self) -> Result<()> {
        if self.block_builder.count() == 0 {
            return Ok(());
        }
        let last_key = self.last_key.clone();
        self.offset += self.block_builder.length();
        let index = TableIndex {
            offset: self.offset,
            key_len: last_key.len() as u64,
            last_key: last_key.clone(),
        };
        self.index_list.push(index);
        let block_builder = std::mem::take(&mut self.block_builder);
        self.block_builder = BlockBuilder::new();
        self.table_file.write_all(&block_builder.build())?;
        Ok(())
    }

    pub fn done(mut self) -> Result<DataLevelIndex> {
        self.write_block()?;
        let last_key = self.last_key;
        let count = self.index_list.len();
        for item in self.index_list {
            self.table_file.write_all(&item.to_bytes())?;
        }
        self.table_file.write_all(&self.offset.to_le_bytes())?;
        self.table_file.write_all(&(count as u64).to_le_bytes())?;
        std::fs::rename(
            &self.path.join(table_file_name_tmp(self.level, self.number)),
            self.path.join(table_file_name(self.level, self.number)),
        )?;
        Ok(DataLevelIndex {
            file_num: self.number,
            start_key: InternalKey::decode(&self.start_key.unwrap().to_bytes())?,
            end_key: InternalKey::decode(&last_key)?,
        })
    }
}
