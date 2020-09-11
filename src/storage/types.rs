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

    fn size(&self) -> usize {
        self.user_key.len() * size_of::<u8>() + size_of::<u64>() + size_of::<ValueType>()
    }
}
