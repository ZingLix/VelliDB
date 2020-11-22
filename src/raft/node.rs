use crate::storage::LocalStorage;
use std::path::PathBuf;

struct Node {
    id: u64,

    storage: LocalStorage,
}

impl Node {
    fn new(id: u64, path: PathBuf, node_list: Vec<String>) -> Node {
        Node {
            id,

            storage: LocalStorage::new(path).unwrap(),
        }
    }
}
