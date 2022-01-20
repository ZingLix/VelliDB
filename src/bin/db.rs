use velli_db::VelliDB;

#[async_std::main]
async fn main() {
    let db = VelliDB::new(config).await.unwrap();
    db.start().await.unwrap();
}
