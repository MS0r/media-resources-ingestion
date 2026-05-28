mod compression;
mod download;
mod execute;
mod handle;
mod types;

pub use execute::*;
pub use types::*;

use async_trait::async_trait;
use std::{path::PathBuf, sync::Arc};

use crate::{
    error::JobErrorOutcome,
    models::AppConfig,
    services::{mongo::MongoService, redis::RedisService},
    storage::{Provider, StorageProvider},
};

type JobId = String;

#[async_trait]
pub trait JobHandler: Send + Sync {
    async fn execute(&self, ctx: &JobContext) -> Result<JobOutcome, JobErrorOutcome>;
}

pub enum JobEnvelope {
    File(FileJob),
    Chunk(ChunkJob),
}

pub struct JobContext {
    pub job_id: JobId,
    pub storage: Arc<dyn StorageProvider>,
    pub db: Arc<MongoService>,
    pub redis: Arc<RedisService>,
    pub config: Arc<AppConfig>,
    pub job: JobEnvelope,
}

impl JobContext {
    pub fn from_file_job(
        job: FileJob,
        db: Arc<MongoService>,
        redis: Arc<RedisService>,
        config: Arc<AppConfig>,
    ) -> Self {
        if let Some(dest) = &job.resource.dest
            && let Some(provider) = &dest.provider
        {
            let storage = provider.into_storage();
            return Self {
                job_id: job._id.to_string(),
                storage,
                db,
                redis,
                config,
                job: JobEnvelope::File(job),
            };
        }

        Self {
            job_id: job._id.to_string(),
            storage: Provider::Local.into_storage(),
            db,
            redis,
            config,
            job: JobEnvelope::File(job),
        }
    }

    pub fn from_chunk_job(
        job: ChunkJob,
        db: Arc<MongoService>,
        redis: Arc<RedisService>,
        config: Arc<AppConfig>,
    ) -> Self {
        Self {
            job_id: job._id.clone(),
            storage: Provider::Local.into_storage(),
            db,
            redis,
            config,
            job: JobEnvelope::Chunk(job),
        }
    }

    pub fn file_job(&self) -> &FileJob {
        match &self.job {
            JobEnvelope::File(j) => j,
            JobEnvelope::Chunk(_) => panic!("Called file_job() on a chunk context"),
        }
    }

    pub fn chunk_job(&self) -> &ChunkJob {
        match &self.job {
            JobEnvelope::Chunk(j) => j,
            JobEnvelope::File(_) => panic!("Called chunk_job() on a file context"),
        }
    }
}

pub enum JobOutcome {
    Completed,
    SpawnedChunks(Vec<ChunkJob>),
}

pub enum JobKind {
    File,
    Chunk,
}

pub(crate) fn expand_path(path: &str, filename: &str) -> PathBuf {
    let expanded_path = shellexpand::tilde(path).to_string();
    PathBuf::from(expanded_path).join(filename)
}
