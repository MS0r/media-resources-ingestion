use crate::models::{Metadata, Resource};
use bb8::Pool;
use bb8_mongodb::MongodbConnectionManager;
use mongodb::{
    Client, Collection, IndexModel, bson::{DateTime, doc}, options::{ClientOptions, IndexOptions}
};
use std::time::Duration;

type MongoPool = Pool<MongodbConnectionManager>;

pub enum UpsertResult {
    Inserted,
    Duplicate(Metadata),
}


#[derive(Clone)]
pub struct MongoService {
    pool: MongoPool,
}

impl MongoService {
    pub async fn new(uri: &str) -> Result<Self, bb8_mongodb::Error> {
        let client_options = ClientOptions::parse(uri).await?;

        let client = Client::with_options(client_options.clone())?;
        let metadata_coll = client.database("ingestion").collection::<Metadata>("files_metadata");
        let resources_coll = client.database("ingestion").collection::<Resource>("resources");

        let file_hash_index = IndexModel::builder()
            .keys(doc! { "file_hash": 1 })
            .options(IndexOptions::builder().unique(true).build())
            .build();
        let storage_index = IndexModel::builder()
            .keys(doc! { "storage_path": 1 })
            .build();
        let provider_index = IndexModel::builder()
            .keys(doc! { "storage_provider": 1 })
            .build();

        let resource_id_index = IndexModel::builder()
            .keys(doc! { "id": 1 })
            .options(IndexOptions::builder().unique(true).build())
            .build();
        
        let resource_batch_id = IndexModel::builder()
            .keys(doc! { "batch_id": 1 })
            .build();

        metadata_coll.create_index(file_hash_index).await?;
        metadata_coll.create_index(storage_index).await?;
        metadata_coll.create_index(provider_index).await?;

        resources_coll.create_index(resource_id_index).await?;
        resources_coll.create_index(resource_batch_id).await?;

        let connection_manager = MongodbConnectionManager::new(client_options, "ingestion");
        let pool = Pool::builder()
            .connection_timeout(Duration::from_secs(10))
            .idle_timeout(Some(Duration::from_secs(60)))
            .max_lifetime(Some(Duration::from_secs(300)))
            .build(connection_manager).await?;

        Ok(Self { pool})
    }

    async fn client(&self) -> Result<bb8::PooledConnection<'_, MongodbConnectionManager>, bb8::RunError<bb8_mongodb::Error>> {
        self.pool.get().await
    }

    pub async fn save_file_metadata(self: &MongoService, metadata: &Metadata) -> Result<(), Box<dyn std::error::Error>> {
        let client = self.client().await?;
        let collection : Collection<Metadata> = client.collection("files_metadata");
        collection.insert_one(metadata).await?;
        Ok(())
    }
    
    pub async fn upsert_file_metadata(
        &self,
        metadata: &Metadata,
    ) -> Result<UpsertResult, Box<dyn std::error::Error>> {
        let client = self.client().await?;
        let collection: Collection<Metadata> = client.collection("files_metadata");

        let filter = doc! { "file_hash": &metadata.file_hash };
        let update = doc! {
            "$set" : {
                "duplicate_reference_count" : 1u32,
                "update_date" : DateTime::now(),
            }
        };

        match collection.find_one_and_update(filter, update).await? {
            None => Ok(UpsertResult::Inserted),
            Some(existing) => Ok(UpsertResult::Duplicate(existing)),
        }
    }

    pub async fn save_resource(&self, resource: &Resource) -> Result<(), Box<dyn std::error::Error>> {
        let client = self.client().await?;
        let collection : Collection<Resource> = client.collection("resources");
        collection.insert_one(resource).await?;
        Ok(())
    }

    pub async fn get_resource_by_id(&self, id: &str) -> Result<Option<Resource>, Box<dyn std::error::Error>> {
        let client = self.client().await?;
        let collection : Collection<Resource> = client.collection("resources");
        let filter = doc! { "id": id };
        let resource = collection.find_one(filter).await?;
        Ok(resource)
    }
}