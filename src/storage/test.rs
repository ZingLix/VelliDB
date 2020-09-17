#[cfg(test)]
mod test {
    use super::super::builder::{self, TableBuilder};
    use super::super::reader::TableReader;
    use super::super::types::{InternalKey, ValueType};
    use crate::Result;
    use tempfile::TempDir;

    #[test]
    fn create_table_file_and_read() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary dir.");
        let mut builder = TableBuilder::new(&temp_dir.path().to_path_buf(), 0, 0)?;
        let create_kvpair = |i: u64| {
            let key =
                InternalKey::new(format!("key{}", i).as_bytes().to_vec(), i, ValueType::Value);
            let value = Some(format!("value{}", i).as_bytes().to_vec());
            (key, value)
        };

        for i in 0..1000 {
            let (k, v) = create_kvpair(i);
            builder.add(&k, &v)?;
        }
        builder.done()?;

        let reader = TableReader::new(
            &temp_dir
                .path()
                .to_path_buf()
                .join("data")
                .join(builder::table_file_name(0, 0)),
        )
        .unwrap();
        let mut count = 0;
        for (key, val) in reader {
            let (k, v) = create_kvpair(count);
            assert_eq!(k, key);
            assert_eq!(v, val);
            count += 1;
        }
        Ok(())
    }
}
