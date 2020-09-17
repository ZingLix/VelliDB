use super::builder::table_file_name;
use super::reader::TableReader;
use super::types::InternalKey;
use crate::Result;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{self, Ordering};

#[derive(Serialize, Deserialize, Clone)]
pub struct DataLevelIndex {
    pub file_num: u64,
    pub start_key: InternalKey,
    pub end_key: InternalKey,
}

#[derive(Serialize, Deserialize, Clone)]
struct DataLevelInfo {
    // data file number
    pub number: u64,
    // index[i] -> the i-th file of this level
    // (file_no, start_key, end_key)
    pub index: Vec<DataLevelIndex>,
}

impl DataLevelInfo {
    pub fn new() -> Self {
        DataLevelInfo {
            number: 0,
            index: vec![],
        }
    }
}

// data_file_index[i] -> level_{i}
type DataFileIndex = Vec<DataLevelInfo>;

pub struct LocalStorageStatus {
    current: CurrentFileContent,
    current_file: File,
    manifest_file: File,
    path: PathBuf,
}

#[derive(Serialize, Deserialize, Debug)]
enum StatusChange {
    IncreaseSeqNum,
    IncreaseLogNum,
    //level
    IncreaseDataFileNum(usize),
}

#[derive(Serialize, Deserialize)]
struct CurrentFileContent {
    pub sequence_num: atomic::AtomicU64,
    pub log_num: u64,
    pub manifest_count: u64,
    pub data_file_index: DataFileIndex,
}

impl CurrentFileContent {
    fn new() -> CurrentFileContent {
        CurrentFileContent {
            sequence_num: atomic::AtomicU64::new(1),
            log_num: 0,
            manifest_count: 1,
            data_file_index: vec![DataLevelInfo::new(); 8],
        }
    }
}

impl LocalStorageStatus {
    pub fn new(path: &PathBuf) -> LocalStorageStatus {
        let (current_file, manifest_file, current_content) = Self::open_current_file(&path);
        let mut status = LocalStorageStatus {
            current: current_content,
            current_file,
            manifest_file,
            path: path.clone(),
        };
        Self::recover_status(&mut status);
        status.manifest_file.seek(SeekFrom::End(0)).unwrap();
        status
    }

    fn manifest_filename(count: u64) -> String {
        return format!("MANIFEST_{}", count);
    }

    fn recover_status(status: &mut LocalStorageStatus) {
        let reader = BufReader::new(status.manifest_file.try_clone().unwrap());
        let mut stream = serde_json::Deserializer::from_reader(reader).into_iter::<StatusChange>();
        let mut seq_num = status.current.sequence_num.load(Ordering::Relaxed);
        while let Some(change) = stream.next() {
            match change {
                Ok(ch) => match ch {
                    StatusChange::IncreaseSeqNum => {
                        seq_num += 1;
                    }
                    StatusChange::IncreaseLogNum => {
                        status.current.log_num += 1;
                    }
                    StatusChange::IncreaseDataFileNum(level) => {
                        status.current.data_file_index[level].number += 1;
                    }
                },
                Err(e) => {
                    print!("{}", e);
                    error!("Deserialize MANIFEST file failed.");
                    std::process::exit(1);
                }
            }
        }
        status
            .current
            .sequence_num
            .store(seq_num, Ordering::Relaxed);
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
                Err(_) => {
                    error!("Create CURRENT file failed.");
                    std::process::exit(1);
                }
            };
            status = CurrentFileContent::new();
            current_file
                .write_all(serde_json::to_string(&status).unwrap().as_bytes())
                .unwrap();
            manifest_file = match OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(path.join(Self::manifest_filename(status.manifest_count)))
            {
                Ok(f) => f,
                Err(_) => {
                    error!(
                        "Create mainfest file {} failed.",
                        Self::manifest_filename(status.manifest_count)
                    );
                    std::process::exit(1);
                }
            };
        } else {
            info!("Found CURRENT file.");
            current_file = match File::open(&current_file_path) {
                Ok(f) => f,
                Err(_) => {
                    error!("Open CURRENT file failed.");
                    std::process::exit(1);
                }
            };
            let mut content = String::new();
            current_file.read_to_string(&mut content).unwrap();
            status = match serde_json::from_str(&content) {
                Ok(s) => s,
                Err(_) => {
                    error!("Deserialize CURRENT file failed. CURRENT file might be corrupted.");
                    std::process::exit(1);
                }
            };
            manifest_file = match OpenOptions::new()
                .read(true)
                .append(true)
                .open(path.join(Self::manifest_filename(status.manifest_count)))
            {
                Ok(f) => f,
                Err(_) => {
                    error!("Open MANIFEST file failed.");
                    std::process::exit(1);
                }
            }
        }
        (current_file, manifest_file, status)
    }

    pub fn next_seq_num(&mut self) -> u64 {
        let seq_num = self.current.sequence_num.fetch_add(1, Ordering::Relaxed);
        let record = serde_json::to_string(&StatusChange::IncreaseSeqNum).unwrap();
        self.manifest_file.write(record.as_bytes()).unwrap();
        seq_num
    }

    pub fn cur_seq_num(&self) -> u64 {
        self.current.sequence_num.load(Ordering::Relaxed)
    }

    pub fn wal_log_num(&self) -> u64 {
        self.current.log_num
    }

    pub fn next_data_file_num(&mut self, level: usize) -> u64 {
        self.current.data_file_index[level].number += 1;
        self.current.data_file_index[level].number
    }

    pub fn add_data_file(&mut self, level: usize, file_index: DataLevelIndex) -> Result<()> {
        self.current.data_file_index[level].index.push(file_index);
        self.update_current_file()?;
        Ok(())
    }

    pub fn update_current_file(&mut self) -> Result<()> {
        self.current.manifest_count += 1;
        let mut new_cur_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(self.path.join("CURRENT_new"))?;
        let new_manifest_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(
                self.path
                    .join(Self::manifest_filename(self.current.manifest_count)),
            )?;
        new_cur_file
            .write_all(serde_json::to_string(&self.current).unwrap().as_bytes())
            .unwrap();
        self.current_file = new_cur_file;
        self.manifest_file = new_manifest_file;
        std::fs::remove_file(self.path.join("CURRENT"))?;
        std::fs::rename(self.path.join("CURRENT_new"), self.path.join("CURRENT"))?;
        Ok(())
    }

    pub fn possible_table_file(&self, level: u8, key: &InternalKey) -> Result<Vec<TableReader>> {
        let table_list = &self.current.data_file_index[level as usize];
        let mut reader_vec = vec![];
        for table in table_list.index.iter() {
            if key.user_key() >= &table.start_key.user_key()
                && key.user_key() <= &table.end_key.user_key()
            {
                reader_vec.push(TableReader::new(
                    &self
                        .path
                        .clone()
                        .join("data")
                        .join(table_file_name(level, table.file_num)),
                )?);
            }
        }
        Ok(reader_vec)
    }
}
