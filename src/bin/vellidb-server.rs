use async_std::fs;
use clap::Parser;
use velli_db::{VelliDB, VelliDBConfig};
extern crate log;
use log::LevelFilter;

#[derive(Parser, Debug)]
struct Args {
    #[clap(short, long)]
    config_file: String,
    #[clap(short, long)]
    node_id: u64,
    #[clap(short, long)]
    address: Option<String>,
    #[clap(short, long)]
    store_path: Option<String>,
}

#[async_std::main]
async fn main() {
    env_logger::builder()
        .filter_level(LevelFilter::Warn)
        .filter(Some("velli"), LevelFilter::Info)
        .init();
    let args = Args::parse();
    let config_file_content = fs::read_to_string(args.config_file).await.unwrap();
    let mut config: VelliDBConfig = toml::from_str(&config_file_content).unwrap();
    config.raft_node_id = Some(args.node_id);
    if let Some(addr) = args.address {
        config.server.address = addr;
    }
    if let Some(store_path) = args.store_path {
        config.server.store_path = store_path;
    }
    let db = VelliDB::new(config).await.unwrap();
    db.start().await.unwrap();
}
