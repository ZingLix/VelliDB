pub fn decode_u64(data: &Vec<u8>) -> u64 {
    let mut buf = [0u8; 8];
    for i in 0..buf.len() {
        buf[i] = data[i];
    }
    return u64::from_le_bytes(buf);
}
