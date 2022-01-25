use async_std::fs;
use clap::Parser;
use velli_db::{VelliDB, VelliDBConfig};

#[derive(Parser, Debug)]
struct Args {
    #[clap(short, long)]
    config_file: String,
    #[clap(short, long)]
    node_id: u32,
}

#[async_std::main]
async fn main() {
    let args = Args::parse();
    let config_file_content = fs::read_to_string(args.config_file).await.unwrap();
    let config: VelliDBConfig = toml::from_str(&config_file_content).unwrap();
    let mut db = VelliDB::new(config).await.unwrap();
    db.start().await.unwrap();
}
