use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{self, Ordering};

pub struct LocalStorageStatus {
    sequence_num: atomic::AtomicU64,
    current_file: File,
    manifest_file: File,
    current_content: CurrentFileContent,
}

#[derive(Serialize, Deserialize, Debug)]
enum StatusChange {
    IncreaseSeqNum,
}

#[derive(Serialize, Deserialize, Debug)]
struct CurrentFileContent {
    sequence_num: u64,
    log_num: u64,
    manifest_count: u64,
}

impl CurrentFileContent {
    fn new() -> CurrentFileContent {
        CurrentFileContent {
            sequence_num: 1,
            log_num: 0,
            manifest_count: 1,
        }
    }
}
fn manifest_filename(count: u64) -> String {
    return format!("MANIFEST_{}", count);
}
impl LocalStorageStatus {
    pub fn new(path: &PathBuf) -> LocalStorageStatus {
        let (current_file, manifest_file, current_content) = Self::open_current_file(&path);
        let mut status = LocalStorageStatus {
            sequence_num: atomic::AtomicU64::new(current_content.sequence_num),
            current_file,
            manifest_file,
            current_content,
        };
        Self::recover_status(&mut status);
        status.manifest_file.seek(SeekFrom::End(0)).unwrap();
        status
    }

    fn recover_status(status: &mut LocalStorageStatus) {
        let reader = BufReader::new(status.manifest_file.try_clone().unwrap());
        let mut stream = serde_json::Deserializer::from_reader(reader).into_iter::<StatusChange>();
        let mut seq_num = status.sequence_num.get_mut().clone();
        while let Some(change) = stream.next() {
            match change {
                Ok(ch) => match ch {
                    StatusChange::IncreaseSeqNum => {
                        seq_num += 1;
                    }
                },
                Err(e) => {
                    error!("Deserialize MANIFEST file failed.");
                    std::process::exit(1);
                }
            }
        }
        status.sequence_num.store(seq_num, Ordering::Relaxed);
    }

    fn open_current_file(path: &PathBuf) -> (File, File, CurrentFileContent) {
        let mut current_file_path = path.clone();
        current_file_path.push("CURRENT");
        let mut current_file: File;
        let manifest_file: File;
        let status: CurrentFileContent;
        if !current_file_path.exists() {
            info!("CURRENT file does not exist.");
            current_file = match File::create(&current_file_path) {
                Ok(f) => f,
                Err(e) => {
                    error!("Create CURRENT file failed.");
                    std::process::exit(1);
                }
            };
            status = CurrentFileContent::new();
            current_file
                .write_all(serde_json::to_string(&status).unwrap().as_bytes())
                .unwrap();
            manifest_file = match File::create(manifest_filename(status.manifest_count)) {
                Ok(f) => f,
                Err(e) => {
                    error!(
                        "Create mainfest file {} failed.",
                        manifest_filename(status.manifest_count)
                    );
                    std::process::exit(1);
                }
            };
        } else {
            info!("Found CURRENT file.");
            current_file = match File::open(&current_file_path) {
                Ok(f) => f,
                Err(e) => {
                    error!("Open CURRENT file failed.");
                    std::process::exit(1);
                }
            };
            let mut content = String::from("");
            current_file.read_to_string(&mut content).unwrap();
            status = match serde_json::from_str(&content) {
                Ok(s) => s,
                Err(e) => {
                    error!("Deserialize CURRENT file failed. CURRENT file might be corrupted.");
                    std::process::exit(1);
                }
            };
            manifest_file = match File::open(manifest_filename(status.manifest_count)) {
                Ok(f) => f,
                Err(e) => {
                    error!("Open MANIFEST file failed.");
                    std::process::exit(1);
                }
            }
        }
        (current_file, manifest_file, status)
    }

    fn next_seq_num(&self) -> u64 {
        self.sequence_num.fetch_add(1, Ordering::Relaxed)
    }

    pub fn wal_log_num(&self) -> u64 {
        self.current_content.log_num
    }
}
