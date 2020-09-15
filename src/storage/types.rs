use crate::{Result, VelliErrorType};
use std::cmp::Ordering;
use std::mem::size_of;

pub type Key = Vec<u8>;

#[derive(PartialEq, Copy, Clone)]
pub enum ValueType {
    Value,
    Deletion,
}

impl From<u8> for ValueType {
    fn from(val: u8) -> Self {
        match val {
            0 => ValueType::Deletion,
            1 => ValueType::Value,
            _ => {
                panic!("Cannot convert {} to ValueType.", val);
            }
        }
    }
}

impl Into<u8> for ValueType {
    fn into(self) -> u8 {
        match self {
            ValueType::Deletion => 0,
            ValueType::Value => 1,
        }
    }
}

pub struct InternalKey {
    user_key: Vec<u8>,
    sequence_num: u64,
    value_type: ValueType,
}

pub type Value = Vec<u8>;

pub type KvPair = (InternalKey, Option<Value>);

impl Clone for InternalKey {
    fn clone(&self) -> Self {
        InternalKey {
            user_key: self.user_key.clone(),
            sequence_num: self.sequence_num,
            value_type: self.value_type,
        }
    }
}

impl InternalKey {
    pub fn new(user_key: Vec<u8>, seq_num: u64, value_type: ValueType) -> InternalKey {
        InternalKey {
            user_key,
            sequence_num: seq_num,
            value_type,
        }
    }

    #[allow(dead_code)]
    fn size(&self) -> usize {
        self.user_key.len() * size_of::<u8>() + size_of::<u64>() + size_of::<ValueType>()
    }

    pub fn user_key(&self) -> &Vec<u8> {
        &self.user_key
    }

    pub fn sequence_num(&self) -> u64 {
        self.sequence_num
    }

    pub fn value_type(&self) -> ValueType {
        self.value_type
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut key = self.user_key.clone();
        key.append(&mut self.sequence_num.to_le_bytes().to_vec());
        let tmp: u8 = self.value_type.into();
        key.push(self.value_type.into());
        key
    }

    pub fn decode(bytes: &Vec<u8>) -> Result<Self> {
        let len = bytes.len();

        let len_without_key = Self::len_without_key();
        if len <= len_without_key {
            Err(VelliErrorType::DecodeError)?;
        }
        let user_key = bytes[..len - Self::len_without_key()].to_vec();
        let mut seq_num_buf = [0u8; 8];
        for i in 0..8 {
            seq_num_buf[i] = bytes[len - len_without_key + i];
        }
        let sequence_num = u64::from_le_bytes(seq_num_buf.clone());
        let value_type: ValueType = bytes.last().unwrap().clone().into();
        Ok(InternalKey {
            user_key,
            sequence_num,
            value_type,
        })
    }

    pub fn len_without_key() -> usize {
        // sequence_num_len(u64) + value_type_len(u8) == 8 + 1
        9
    }
}

impl PartialEq for InternalKey {
    fn eq(&self, other: &Self) -> bool {
        self.sequence_num.cmp(&other.sequence_num) == Ordering::Equal
            && self.user_key.cmp(&other.user_key) == Ordering::Equal
    }
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> Ordering {
        let mut order = self.user_key.cmp(&other.user_key);
        if order == Ordering::Equal {
            order = self.sequence_num.cmp(&other.sequence_num);
        }
        order
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for InternalKey {}
