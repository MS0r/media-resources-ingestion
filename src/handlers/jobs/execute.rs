use async_trait::async_trait;

use crate::{error::JobErrorOutcome, services::mongo::UpsertResult};

use super::{
    JobContext, JobOutcome,
    download::{download_to_temp, extract_file_job, initiate_download},
    handle::{handle_duplicate, handle_new_file},
};

pub struct FileJobHandler;

pub struct ChunkJobHandler;

#[async_trait]
impl super::JobHandler for FileJobHandler {
    async fn execute(&self, ctx: &JobContext) -> Result<JobOutcome, JobErrorOutcome> {
        let file_job = extract_file_job(&ctx.job)?;
        let (response, mut download) = initiate_download(&file_job.resource).await?;

        let threshold_bytes = ctx.config.compression_threshold_mb * 1024 * 1024;

        let temp_dir = &ctx.config.temp_dir;
        let (temp_path, hash_hex, detected_mime, actual_size) =
            download_to_temp(response, temp_dir, &download.filename, &ctx.job_id).await?;

        if download.content_length == 0 && actual_size > 0 {
            tracing::info!(
                "Content-Length was 0 — using actual streamed size: {} bytes",
                actual_size
            );
            download.content_length = actual_size;
            if download.content_length > threshold_bytes {
                tracing::info!(
                    "Large file ({} bytes), spawning chunks",
                    download.content_length
                );
                tokio::fs::remove_file(&temp_path).await.ok();
                return Ok(JobOutcome::SpawnedChunks(vec![]));
            }
        }

        if detected_mime != "application/octet-stream" {
            download.mime_type = detected_mime;
        }

        match ctx.db.upsert_file_metadata(&hash_hex).await {
            Ok(UpsertResult::Inserted) => {
                handle_new_file(ctx, file_job, download, temp_path, hash_hex).await
            }
            Ok(UpsertResult::Duplicate(existing)) => {
                handle_duplicate(&temp_path, &existing.file_hash).await
            }
            Err(e) => {
                tokio::fs::remove_file(&temp_path).await.ok();
                Err(JobErrorOutcome::from(e))
            }
        }
    }
}

#[async_trait]
impl super::JobHandler for ChunkJobHandler {
    async fn execute(&self, ctx: &JobContext) -> Result<JobOutcome, JobErrorOutcome> {
        let chunk_job = ctx.chunk_job();

        tracing::info!(job_id = %ctx.job_id, "Processing chunk {}", chunk_job.chunk_index);

        ctx.redis
            .register_chunk(&chunk_job.file_hash, &ctx.job_id)
            .await?;

        tracing::info!(job_id = %ctx.job_id, "Chunk {} completed", chunk_job.chunk_index);
        Ok(JobOutcome::Completed)
    }
}

#[cfg(test)]
mod tests {
    use super::super::{
        JobEnvelope,
        download::extract_file_job,
        download::filename_from_url,
        expand_path,
        types::{ChunkJob, FileJob, JobStatus},
    };
    use chrono::Utc;
    use std::path::PathBuf;
    use url::Url;

    use crate::models::Resource;

    #[test]
    fn test_filename_from_url_standard() {
        let url = Url::parse("https://example.com/images/photo.png").unwrap();
        assert_eq!(
            filename_from_url(&url),
            (Some("photo".into()), Some("png".into()))
        );
    }

    #[test]
    fn test_filename_from_url_no_path() {
        let url = Url::parse("https://example.com").unwrap();
        assert_eq!(filename_from_url(&url), (None, None));
    }

    #[test]
    fn test_filename_from_url_root() {
        let url = Url::parse("https://example.com/").unwrap();
        assert_eq!(filename_from_url(&url), (None, None));
    }

    #[test]
    fn test_filename_from_url_deep_path() {
        let url = Url::parse("https://cdn.example.com/a/b/c/d/file.txt?query=1").unwrap();
        assert_eq!(
            filename_from_url(&url),
            (Some("file".into()), Some("txt".into()))
        );
    }

    #[test]
    fn test_filename_from_url_trailing_slash() {
        let url = Url::parse("https://example.com/dir/").unwrap();
        assert_eq!(filename_from_url(&url), (Some("dir".into()), None));
    }

    #[test]
    fn test_expand_path_simple() {
        let result = expand_path("/base", "file.txt");
        assert_eq!(result, PathBuf::from("/base/file.txt"));
    }

    #[test]
    fn test_expand_path_nested() {
        let result = expand_path("/base/dir", "sub/file.txt");
        assert_eq!(result, PathBuf::from("/base/dir/sub/file.txt"));
    }

    #[test]
    fn test_expand_path_tilde_root() {
        let home = dirs::home_dir().expect("home dir should exist in test env");
        let result = expand_path("~/downloads", "file.txt");
        assert_eq!(result, home.join("downloads/file.txt"));
    }

    #[test]
    fn test_expand_path_tilde_only() {
        let home = dirs::home_dir().expect("home dir should exist in test env");
        let result = expand_path("~", "file.txt");
        assert_eq!(result, home.join("file.txt"));
    }

    #[test]
    fn test_extract_file_job_file() {
        let file_job = FileJob {
            _id: "j1".into(),
            batch_id: "b1".into(),
            resource: Resource {
                id: "r1".into(),
                url: Url::parse("https://example.com/f.png").unwrap(),
                name: None,
                priority: None,
                dest: None,
                config: None,
            },
            priority: 0,
            status: JobStatus::Pending,
            retry_count: 0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            file_hash: None,
            error: None,
        };
        let envelope = JobEnvelope::File(file_job);
        let result = extract_file_job(&envelope);
        assert!(result.is_ok());
        assert_eq!(result.unwrap()._id, "j1");
    }

    #[test]
    fn test_extract_file_job_chunk_fails() {
        let chunk_job = ChunkJob {
            _id: "c1".into(),
            parent_job_id: "j1".into(),
            file_hash: "abc".into(),
            chunk_index: 0,
            offset_start: 0,
            offset_end: 99,
            priority: 0,
            status: JobStatus::Pending,
            retry_count: 0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            chunk_hash: None,
            error: None,
        };
        let envelope = JobEnvelope::Chunk(chunk_job);
        let result = extract_file_job(&envelope);
        assert!(result.is_err());
    }
}
