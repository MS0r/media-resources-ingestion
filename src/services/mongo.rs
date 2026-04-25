use bb8::Pool;
use bb8_mongodb::MongodbConnectionManager;
use mongodb::options::ClientOptions;

type MongoPool = Pool<MongodbConnectionManager>;

#[derive(Clone)]
pub struct MongoService {
    pool: MongoPool,
}

impl MongoService {
    pub async fn new(uri: &str) -> Result<Self, bb8_mongodb::Error> {
        let client_options = ClientOptions::parse(uri).await?;
        let connection_manager = MongodbConnectionManager::new(client_options, "ingestion");
        let pool = Pool::builder().build(connection_manager).await?;
        Ok(Self { pool })
    }

    pub async fn client(&self) -> Result<bb8::PooledConnection<'_, MongodbConnectionManager>, bb8::RunError<bb8_mongodb::Error>> {
        self.pool.get().await
    }
}