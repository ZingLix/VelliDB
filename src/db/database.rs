use async_std::channel::{self, Sender};
use async_std::sync::Mutex;
use bincode;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tide::{Request, Response};

use super::config::{ServerConfig, VelliDBConfig};
use super::options;
use crate::raft::create_raft_node;
use crate::raft::{RaftNode, RaftNodeHandle, RaftProposeResult};
use crate::{LocalStorage, Result, VelliErrorType};

#[derive(Serialize, Deserialize)]
pub enum DBOperator {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

async fn delete(req: Request<VelliDBHandle>) -> tide::Result<Response> {
    let handle = req.state();
    let key = req.param("key").unwrap();
    let operator = DBOperator::Delete(key.as_bytes().to_vec());

    propose(&handle.raft_handle, operator).await
}

async fn put(mut req: Request<VelliDBHandle>) -> tide::Result<Response> {
    let key = req.param("key").unwrap().as_bytes().to_vec();
    let handle = req.state().clone();
    let value = req.body_bytes().await?;
    let operator = DBOperator::Put(key, value);

    propose(&handle.raft_handle, operator).await
}

async fn get(req: Request<VelliDBHandle>) -> tide::Result<Response> {
    let key = req.param("key").unwrap().as_bytes().to_vec();
    let handle = req.state();
    let guard = handle.storage.lock().await;
    match guard.get(key).await {
        Ok(v) => {
            let value = match v {
                Some(v) => v,
                None => vec![],
            };
            Ok(Response::builder(200).body(value).build())
        }
        Err(_) => Ok(Response::builder(400)
            .body(r#"{"error": "true", "msg": "Unknown exception..."}"#)
            .build()),
    }
}

async fn propose(handle: &RaftNodeHandle, operate: DBOperator) -> tide::Result<Response> {
    match handle.propose(bincode::serialize(&operate).unwrap()).await {
        Ok(r) => match r {
            RaftProposeResult::Success(_) => Ok(Response::builder(200).build()),
            _ => Ok(Response::builder(400)
                .body(r#"{"error": "true", "msg": "Propose failed, try again..."}"#)
                .build()),
        },
        Err(_) => Ok(Response::builder(400)
            .body(r#"{"error": "true", "msg": "Unknown exception..."}"#)
            .build()),
    }
}

pub struct VelliDB {
    server: Option<tide::Server<VelliDBHandle>>,
    config: ServerConfig,
    storage: Arc<Mutex<LocalStorage>>,
    raft_node: RaftNode,
    running: Option<Sender<()>>,
}

impl VelliDB {
    pub async fn new(mut config: VelliDBConfig) -> Result<VelliDB> {
        let mut path = PathBuf::new();
        path.push(config.server.store_path.clone());
        path.push(options::STORAGE_BASE_PATH);
        async_std::fs::create_dir_all(path.clone()).await?;
        let self_info_idx = config
            .raft_node
            .iter()
            .position(|x| x.id == config.raft_node_id.unwrap());
        let self_info;
        match self_info_idx {
            Some(idx) => {
                self_info = config.raft_node.remove(idx);
            }
            None => {
                panic!(
                    "Node {} not found in node list.",
                    config.raft_node_id.unwrap()
                );
            }
        }
        path.pop();
        path.push(options::STORAGE_RAFT_FOLDER_NAME);
        async_std::fs::create_dir_all(path.clone()).await?;
        let raft_node = create_raft_node(path.clone(), self_info, config.raft_node);
        let raft_node = raft_node.start()?;

        path.pop();
        path.push(options::STORAGE_DB_FOLDER_NAME);
        async_std::fs::create_dir_all(path.clone()).await?;
        let storage = Arc::new(Mutex::new(LocalStorage::new(path).await?));

        let mut db = VelliDB {
            server: None,
            config: config.server,
            storage,
            raft_node,
            running: None,
        };
        let db_handle = db.handle();
        let mut server = tide::with_state(db_handle);
        server.at("/:key").put(put);
        server.at("/:key").delete(delete);
        server.at("/:key").get(get);
        db.server = Some(server);
        Ok(db)
    }

    pub async fn start(mut self) -> Result<()> {
        match &self.running {
            Some(_) => {
                warn!("The db has already been running!");
                return Err(VelliErrorType::InvalidArguments)?;
            }
            None => {}
        }
        let server_handle =
            async_std::task::spawn(self.server.unwrap().listen(self.config.address));
        let (sender, receiver) = channel::bounded(1);
        self.running = Some(sender);
        loop {
            let l = self.raft_node.new_log().await?;
            let operate: DBOperator = bincode::deserialize(&l.content.unwrap()).unwrap();
            {
                let mut storage = self.storage.lock().await;
                match operate {
                    DBOperator::Put(k, v) => {
                        storage.set(k, v).await?;
                    }
                    DBOperator::Delete(k) => {
                        storage.delete(k).await?;
                    }
                }
            }
            match receiver.try_recv() {
                Ok(_) => {
                    break;
                }
                Err(_) => {}
            }
        }
        server_handle.await?;
        Ok(())
    }

    pub async fn stop(&mut self) -> Result<()> {
        match &self.running {
            Some(sx) => {
                info!("The db is told to stop.");
                sx.send(()).await?;
                Ok(())
            }
            None => {
                warn!("The db is not running!");
                Err(VelliErrorType::InvalidArguments)?
            }
        }
    }

    fn handle(&self) -> VelliDBHandle {
        VelliDBHandle {
            raft_handle: self.raft_node.handle(),
            storage: Arc::clone(&self.storage),
        }
    }
}

#[derive(Clone)]
struct VelliDBHandle {
    pub raft_handle: RaftNodeHandle,
    pub storage: Arc<Mutex<LocalStorage>>,
}
