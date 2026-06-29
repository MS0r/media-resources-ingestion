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
    models::{AppConfig, ChunkRef, Metadata},
    services::redis::ProgressReporter,
    services::{mongo::MongoService, redis::RedisService},
    storage::{Provider, ProviderCache, StorageProvider},
};

#[async_trait]
pub trait JobHandler: Send + Sync {
    async fn execute(&self, ctx: &JobContext) -> Result<JobOutcome, JobErrorOutcome>;
}

pub enum JobEnvelope {
    File(FileJob),
    Chunk(ChunkJob),
}

pub struct JobContext {
    pub storage: Arc<dyn StorageProvider>,
    pub db: Arc<MongoService>,
    pub redis: Arc<RedisService>,
    pub config: Arc<AppConfig>,
    pub job: JobEnvelope,
    pub progress: Option<ProgressReporter>,
    pub http_client: Arc<wreq::Client>,
    pub auth_token: Option<String>,
}

impl JobContext {
    pub fn from_file_job(
        job: FileJob,
        db: Arc<MongoService>,
        redis: Arc<RedisService>,
        config: Arc<AppConfig>,
        http_client: Arc<wreq::Client>,
        auth_token: Option<String>,
        provider_cache: &ProviderCache,
    ) -> Self {
        let progress = Some(ProgressReporter::new(job._id.clone(), (*redis).clone()));
        let storage = if let Some(dest) = &job.resource.dest
            && let Some(provider) = &dest.provider
        {
            provider_cache.get(provider)
        } else {
            provider_cache.get(&Provider::Local)
        };

        Self {
            storage,
            db,
            redis,
            config,
            job: JobEnvelope::File(job),
            progress,
            http_client,
            auth_token,
        }
    }

    pub fn from_chunk_job(
        job: ChunkJob,
        db: Arc<MongoService>,
        redis: Arc<RedisService>,
        config: Arc<AppConfig>,
        http_client: Arc<wreq::Client>,
        provider_cache: &ProviderCache,
    ) -> Self {
        Self {
            storage: provider_cache.get(&job.storage),
            db,
            redis,
            config,
            job: JobEnvelope::Chunk(job),
            progress: None,
            http_client,
            auth_token: None,
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
    Completed(Metadata),
    Duplicated,
    SpawnedChunks(Vec<ChunkJob>),
    ChunkCompleted(ChunkRef, String),
}

#[derive(Clone, Debug, PartialEq)]
pub enum JobKind {
    File,
    Chunk,
}

pub(crate) fn expand_path(path: &str, filename: &str) -> PathBuf {
    let expanded_path = shellexpand::tilde(path).to_string();
    PathBuf::from(expanded_path).join(filename)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expand_path_simple() {
        let result = expand_path("/tmp/ingest", "file.bin");
        assert_eq!(result, PathBuf::from("/tmp/ingest/file.bin"));
    }

    #[test]
    fn test_expand_path_trailing_slash() {
        let result = expand_path("/tmp/ingest/", "file.bin");
        assert_eq!(result, PathBuf::from("/tmp/ingest/file.bin"));
    }

    #[test]
    fn test_expand_path_tilde() {
        let home = std::env::var("HOME").unwrap();
        let result = expand_path("~/ingest", "file.bin");
        assert_eq!(result, PathBuf::from(format!("{}/ingest/file.bin", home)));
    }

    #[test]
    fn test_expand_path_nested_filename() {
        let result = expand_path("/base", "subdir/file.bin");
        assert_eq!(result, PathBuf::from("/base/subdir/file.bin"));
    }

    #[test]
    fn test_expand_path_root() {
        let result = expand_path("/", "file.bin");
        assert_eq!(result, PathBuf::from("/file.bin"));
    }

    #[test]
    fn test_expand_path_empty_path() {
        let result = expand_path("", "file.bin");
        assert_eq!(result, PathBuf::from("file.bin"));
    }

    #[test]
    fn test_expand_path_chunk_label() {
        let result = expand_path("/data/output/resource", "chunk_00000.bin");
        assert_eq!(
            result,
            PathBuf::from("/data/output/resource/chunk_00000.bin")
        );
    }
}
