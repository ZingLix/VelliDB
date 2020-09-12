use std::cmp::Ordering;
use std::mem::size_of;
pub type Key = Vec<u8>;

#[derive(PartialEq, Copy, Clone)]
pub enum ValueType {
    Value,
    Deletion,
}

pub struct InternalKey {
    user_key: Vec<u8>,
    sequence_num: u64,
    value_type: ValueType,
}

pub type Value = Vec<u8>;

pub type KvPair = (InternalKey, Option<Value>);

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
