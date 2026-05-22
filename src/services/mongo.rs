use crate::{
    cli::JobStatus as JobStatusCli,
    error::ToolError,
    handlers::jobs::{Batch, ChunkJob, FileJob, JobStatus},
    models::Metadata,
};
use bb8::Pool;
use bb8_mongodb::MongodbConnectionManager;
use chrono::Utc;
use futures_util::stream::StreamExt;
use mongodb::{
    Client, Collection, IndexModel,
    bson::{DateTime, doc, serialize_to_bson},
    options::{ClientOptions, IndexOptions},
};
use std::time::Duration;

type MongoPool = Pool<MongodbConnectionManager>;

pub enum UpsertResult {
    Inserted,
    Duplicate(Box<Metadata>),
}

#[derive(Clone)]
pub struct MongoService {
    pool: MongoPool,
}

impl MongoService {
    pub async fn new(uri: &str) -> Result<Self, bb8_mongodb::Error> {
        let client_options = ClientOptions::parse(uri).await?;

        let client = Client::with_options(client_options.clone())?;
        let metadata_coll = client
            .database("ingestion")
            .collection::<Metadata>("files_metadata");
        let _batches_coll = client.database("ingestion").collection::<Batch>("batches");
        let files_jobs_coll = client
            .database("ingestion")
            .collection::<FileJob>("files_jobs");
        let chunks_jobs_coll = client
            .database("ingestion")
            .collection::<ChunkJob>("chunks_jobs");

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

        let original_url_index = IndexModel::builder()
            .keys(doc! { "original_url": 1 })
            .build();

        metadata_coll.create_index(file_hash_index).await?;
        metadata_coll.create_index(storage_index).await?;
        metadata_coll.create_index(provider_index).await?;
        metadata_coll.create_index(original_url_index).await?;

        let batch_id = IndexModel::builder().keys(doc! { "batch_id": 1 }).build();

        files_jobs_coll.create_index(batch_id).await?;

        let parent_job_id_index = IndexModel::builder()
            .keys(doc! { "parent_job_id": 1 })
            .build();

        chunks_jobs_coll.create_index(parent_job_id_index).await?;

        let connection_manager = MongodbConnectionManager::new(client_options, "ingestion");
        let pool = Pool::builder()
            .connection_timeout(Duration::from_secs(10))
            .idle_timeout(Some(Duration::from_secs(60)))
            .max_lifetime(Some(Duration::from_secs(300)))
            .build(connection_manager)
            .await?;

        Ok(Self { pool })
    }

    async fn client(
        &self,
    ) -> Result<
        bb8::PooledConnection<'_, MongodbConnectionManager>,
        bb8::RunError<bb8_mongodb::Error>,
    > {
        self.pool.get().await
    }

    pub async fn complete_job(
        self: &MongoService,
        metadata: Metadata,
        job_id: &str,
    ) -> Result<(), ToolError> {
        let client = self.client().await?;
        let collection: Collection<Metadata> = client.collection("files_metadata");
        collection.insert_one(&metadata).await?;

        let complete_job = serialize_to_bson(&JobStatus::Completed {
            finished_at: Utc::now(),
        })?;
        let jobs_collection: Collection<FileJob> = client.collection("files_jobs");
        jobs_collection
            .update_one(
                doc! { "_id": job_id },
                doc! { "$set": { "status": complete_job, "file_hash": metadata.file_hash } },
            )
            .await?;

        Ok(())
    }

    pub async fn upsert_file_metadata(&self, file_hash: &str) -> Result<UpsertResult, ToolError> {
        let client = self.client().await?;
        let collection: Collection<Metadata> = client.collection("files_metadata");

        let filter = doc! { "file_hash": file_hash };
        let update = doc! {
            "$set" : {
                "duplicate_reference_count" : 1u32,
                "update_date" : DateTime::now(),
            }
        };

        match collection.find_one_and_update(filter, update).await? {
            None => Ok(UpsertResult::Inserted),
            Some(existing) => Ok(UpsertResult::Duplicate(Box::new(existing))),
        }
    }

    pub async fn save_batch(&self, batch: Batch) -> Result<(), ToolError> {
        let client = self.client().await?;
        let collection: Collection<Batch> = client.collection("batches");
        collection.insert_one(batch).await?;
        Ok(())
    }

    pub async fn save_file_job(&self, file_job: FileJob) -> Result<(), ToolError> {
        let client = self.client().await?;
        let collection: Collection<FileJob> = client.collection("files_jobs");
        match collection.insert_one(&file_job).await {
            Ok(_) => return Ok(()),
            Err(e) if e.to_string().contains("E11000 duplicate key error") => {
                return Err(format!("File job with ID {} already exists", file_job._id).into());
            }
            Err(e) => return Err(e.into()),
        }
    }

    pub async fn save_chunk_job(&self, chunk_job: ChunkJob) -> Result<(), ToolError> {
        let client = self.client().await?;
        let collection: Collection<ChunkJob> = client.collection("chunks_jobs");
        collection.insert_one(chunk_job).await?;
        Ok(())
    }

    pub async fn get_batch(&self, batch_id: &str) -> Result<Option<Batch>, ToolError> {
        let client = self.client().await?;
        let collection: Collection<Batch> = client.collection("batches");
        let batch = collection.find_one(doc! { "_id": batch_id }).await?;
        Ok(batch)
    }

    pub async fn get_file_job(&self, job_id: &str) -> Result<Option<FileJob>, ToolError> {
        let client = self.client().await?;
        let collection: Collection<FileJob> = client.collection("files_jobs");
        let job = collection.find_one(doc! { "_id": job_id }).await?;
        Ok(job)
    }

    pub async fn get_chunk_job(&self, job_id: &str) -> Result<Option<ChunkJob>, ToolError> {
        let client = self.client().await?;
        let collection: Collection<ChunkJob> = client.collection("chunks_jobs");
        let job = collection.find_one(doc! { "_id": job_id }).await?;
        Ok(job)
    }

    pub async fn list_jobs(
        &self,
        filter_status: Option<JobStatusCli>,
        limit: usize,
    ) -> Result<Vec<FileJob>, ToolError> {
        let client = self.client().await?;
        let collection: Collection<FileJob> = client.collection("files_jobs");

        let mut filter = doc! {};
        if let Some(status) = filter_status {
            let status_str = match status {
                JobStatusCli::Pending => "status.pending",
                JobStatusCli::Running => "status.running",
                JobStatusCli::Completed => "status.completed",
                JobStatusCli::Failed => "status.failed",
                JobStatusCli::Retrying => "status.retrying",
                JobStatusCli::Cancelled => "status.cancelled",
            };
            filter.insert(status_str, doc! {"$exists" : true});
        }

        let mut cursor = collection.find(filter).limit(limit as i64).await?;
        let mut jobs = Vec::new();
        while let Some(result) = cursor.next().await {
            jobs.push(result?);
        }
        Ok(jobs)
    }

    pub async fn list_files(
        &self,
        mime_filter: Option<&str>,
        provider_filter: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Metadata>, ToolError> {
        let client = self.client().await?;
        let collection: Collection<Metadata> = client.collection("files_metadata");

        let mut filter = doc! {};
        if let Some(mime) = mime_filter {
            filter.insert("mime_type", mime);
        }
        if let Some(provider) = provider_filter {
            filter.insert("storage_provider", provider);
        }

        let mut cursor = collection.find(filter).limit(limit as i64).await?;
        let mut files = Vec::new();
        while let Some(result) = cursor.next().await {
            files.push(result?);
        }
        Ok(files)
    }

    pub async fn get_file_metadata(&self, file_hash: &str) -> Result<Option<Metadata>, ToolError> {
        let client = self.client().await?;
        let collection: Collection<Metadata> = client.collection("files_metadata");
        let file = collection.find_one(doc! { "file_hash": file_hash }).await?;
        Ok(file)
    }

    pub async fn cancel_batch_jobs(&self, batch_id: &str) -> Result<usize, ToolError> {
        let client = self.client().await?;
        let collection: Collection<FileJob> = client.collection("files_jobs");

        let pending_job = serialize_to_bson(&JobStatus::Pending)?;
        let result = collection
            .delete_many(doc! { "batch_id": batch_id, "status": pending_job })
            .await?;
        Ok(result.deleted_count as usize)
    }

    pub async fn cancel_job(&self, job_id: &str) -> Result<bool, ToolError> {
        let client = self.client().await?;
        let collection: Collection<FileJob> = client.collection("files_jobs");

        let result = collection
            .delete_one(doc! { "_id": job_id, "status": "pending" })
            .await?;
        Ok(result.deleted_count > 0)
    }

    pub async fn retry_failed_job(&self, job_id: &str) -> Result<bool, ToolError> {
        let client = self.client().await?;
        let collection: Collection<FileJob> = client.collection("files_jobs");

        let pending_job = serialize_to_bson(&JobStatus::Pending)?;

        let result = collection
            .update_one(
                doc! { "_id": job_id, "status.failed" : doc! {"$exists" : true} },
                doc! { "$set": { "status": pending_job, "retry_count": 0 } },
            )
            .await?;
        Ok(result.modified_count > 0)
    }

    pub async fn fail_job(&self, job_id: &str, reason: &str) -> Result<(), ToolError> {
        let client = self.client().await?;
        let collection: Collection<FileJob> = client.collection("files_jobs");

        let failed_job = serialize_to_bson(&JobStatus::Failed {
            reason: reason.to_string(),
            failed_at: Utc::now(),
        })?;
        collection
            .update_one(
                doc! { "_id": job_id },
                doc! { "$set": { "status": failed_job } },
            )
            .await?;
        Ok(())
    }
}
