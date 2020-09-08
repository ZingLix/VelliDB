pub type Key = Vec<u8>;
pub struct InternalKey {
    user_key: Key,
    sequence_num: u64,
}
