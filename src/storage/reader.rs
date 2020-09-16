use super::builder::TableIndex;
use super::types::{InternalKey, KvPair, Value};
use crate::util::utils;
use crate::{Result, VelliErrorType};
use std::fs::{self, File};
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::PathBuf;

pub struct TableReader {
    table_file_reader: BufReader<File>,
    block_reader: Option<BlockReader>,
    index_offset: Vec<TableIndex>,
    idx: usize,
    cur: Option<KvPair>,
}

impl TableReader {
    pub fn new(path: &PathBuf) -> Result<Self> {
        let file = File::open(path).unwrap();
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::End(16))?;
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf)?;
        let index_offset = u64::from_le_bytes(buf.clone());
        reader.read_exact(&mut buf)?;
        let index_count = u64::from_le_bytes(buf.clone());
        reader.seek(SeekFrom::Start(index_offset))?;
        let mut index_offset = vec![];
        for _ in 0..index_count {
            reader.read_exact(&mut buf)?;
            let offset = u64::from_le_bytes(buf.clone());
            reader.read_exact(&mut buf)?;
            let key_len = u64::from_le_bytes(buf.clone());
            let mut v = vec![0u8; key_len as usize];
            reader.read_exact(&mut v)?;
            index_offset.push(TableIndex {
                offset,
                key_len,
                last_key: v,
            })
        }
        index_offset.pop();
        reader.seek(SeekFrom::Start(0))?;

        Ok(TableReader {
            table_file_reader: reader,
            block_reader: None,
            index_offset,
            idx: 0,
            cur: None,
        })
    }
}

impl Iterator for TableReader {
    type Item = KvPair;

    fn next(&mut self) -> Option<Self::Item> {
        let mut kv = None;
        if !self.block_reader.is_none() {
            kv = self.block_reader.as_mut().unwrap().next();
        }
        // first or current block_reader comes to the end
        if self.block_reader.is_none() || kv.is_none() {
            if self.idx == self.index_offset.len() {
                return None;
            }
            let start_offset = if self.idx == 0 {
                0
            } else {
                self.index_offset[self.idx - 1].offset
            };
            let end_offset = self.index_offset[self.idx].offset;
            let mut buffer = vec![0u8; (end_offset - start_offset) as usize];
            self.table_file_reader.read_exact(&mut buffer).unwrap();
            self.block_reader = Some(BlockReader::new(buffer));
            self.idx += 1;
            kv = self.block_reader.as_mut().unwrap().next();
        }

        self.cur = kv.clone();
        kv
    }
}

impl TableReader {
    pub fn upper_bound(&mut self, key: &InternalKey) -> Result<()> {
        let key_bytes = key.to_bytes();
        let mut block_idx = self.index_offset.len();
        for idx in 0..self.index_offset.len() + 1 {
            if idx == self.index_offset.len() {
                self.idx = idx;
                self.cur = None;
                return Ok(());
            }
            if key_bytes <= self.index_offset[idx].last_key {
                block_idx = idx;
                break;
            }
        }
        self.cur = None;
        self.idx = block_idx;
        self.next();
        self.block_reader.as_mut().unwrap().upper_bound(key)?;
        Ok(())
    }

    pub fn cur(&self) -> Option<KvPair> {
        match &self.block_reader {
            Some(reader) => reader.current(),
            None => None,
        }
    }
}

struct BlockReader {
    bytes: Vec<u8>,
    offset: usize,
    restart_offset: Vec<u64>,
    last_entry_key: Vec<u8>,
    cur: Option<KvPair>,
}

impl BlockReader {
    fn new(bytes: Vec<u8>) -> Self {
        let mut restart_offset = vec![];
        let mut offset = 0;
        let entry_count = utils::decode_u64(&bytes[0..8].to_vec());
        offset += 8;
        for _ in 0..entry_count as usize {
            let res_offset = utils::decode_u64(&bytes[offset..offset + 8].to_vec());
            offset += 8;
            restart_offset.push(res_offset);
        }
        BlockReader {
            bytes,
            offset,
            restart_offset,
            last_entry_key: vec![],
            cur: None,
        }
    }
}

impl Iterator for BlockReader {
    type Item = KvPair;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.peek_next();
        self.update_next(next)
    }
}

impl BlockReader {
    fn peek_next(&self) -> Option<(KvPair, usize, Vec<u8>)> {
        let mut offset = self.offset;
        if offset >= self.bytes.len() {
            return None;
        }
        let mut count = [0u64; 3];
        for i in 0..count.len() {
            count[i] = utils::decode_u64(&self.bytes[offset..offset + 8].to_vec());
            offset += 8;
        }
        let shared_bytes = count[0] as usize;
        let unshared_bytes = count[1] as usize;
        let value_bytes = count[2] as usize;
        let mut unshared_key_data = self.bytes[offset..offset + unshared_bytes].to_vec();
        offset += unshared_bytes;
        let value_data = self.bytes[offset..offset + value_bytes].to_vec();
        let mut key = self.last_entry_key[0..shared_bytes].to_vec();
        key.append(&mut unshared_key_data);
        let ikey = match InternalKey::decode(&self.last_entry_key) {
            Ok(k) => k,
            Err(_) => {
                error!("Decode block failed.");
                std::process::exit(1);
            }
        };
        Some(((ikey, Some(value_data)), offset, key))
    }

    fn update_next(&mut self, next: Option<(KvPair, usize, Vec<u8>)>) -> Option<KvPair> {
        if let Some((kv, offset, key_bytes)) = next {
            self.offset = offset;
            self.last_entry_key = key_bytes;
            self.cur = Some(kv.clone());
            return Some(kv);
        } else {
            self.cur = None;
            return None;
        }
    }

    fn read_restart_head(&self, index: usize) -> Result<KvPair> {
        let mut offset = self.restart_offset[index] as usize;
        let mut count = [0u64; 3];
        for i in 0..count.len() {
            count[i] = utils::decode_u64(&self.bytes[offset..offset + 8].to_vec());
            offset += 8;
        }
        let shared_bytes = count[0] as usize;
        let unshared_bytes = count[1] as usize;
        let value_bytes = count[2] as usize;
        let mut key = self.bytes[offset..offset + unshared_bytes].to_vec();
        offset += unshared_bytes;
        if value_bytes != 0 {
            let value_data = self.bytes[offset..offset + value_bytes].to_vec();
            Ok((InternalKey::decode(&key)?, Some(value_data)))
        } else {
            Ok((InternalKey::decode(&key)?, None))
        }
    }

    pub fn upper_bound(&mut self, key: &InternalKey) -> Result<()> {
        let size = self.find_upper_bound(key.clone(), 0, self.restart_offset.len(), None, None)?;
        self.offset = size;
        self.cur = None;
        while let Some(kv) = self.peek_next() {
            if &(kv.0).0 > key {
                break;
            }
            self.update_next(Some(kv));
        }
        Ok(())
    }

    fn find_upper_bound(
        &self,
        key: InternalKey,
        start: usize,
        end: usize,
        mut start_result: Option<InternalKey>,
        mut end_result: Option<InternalKey>,
    ) -> Result<usize> {
        if end - start <= 1 {
            return Ok(start);
        }
        let middle = (start + end) / 2;
        let middle_result = Some(self.read_restart_head(middle)?.0);
        if start_result.is_none() {
            start_result = Some(self.read_restart_head(start)?.0);
        }
        if start_result.as_ref().unwrap() <= &key && &key < middle_result.as_ref().unwrap() {
            return self.find_upper_bound(key, start, middle, start_result, middle_result);
        }
        if end_result.is_none() {
            end_result = Some(self.read_restart_head(end)?.0);
        }
        if middle_result.as_ref().unwrap() <= &key && &key <= end_result.as_ref().unwrap() {
            return self.find_upper_bound(key, middle, end, middle_result, end_result);
        }
        Err(VelliErrorType::InvalidArguments)?
    }

    pub fn current(&self) -> Option<KvPair> {
        self.cur.clone()
    }
}
