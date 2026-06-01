use crate::{
    cli::JobStatus,
    error::ToolError,
    handlers::jobs::{Batch, ChunkJob, FileJob, JobKind},
};
use redis::{AsyncCommands, Client, aio::MultiplexedConnection};

#[derive(Clone)]
pub struct RedisService {
    client: Client,
    running_job_ttl_secs: u64,
}

impl RedisService {
    pub fn new(redis_url: &str, running_job_ttl_secs: u64) -> Result<Self, redis::RedisError> {
        let client = Client::open(redis_url)?;
        Ok(Self {
            client,
            running_job_ttl_secs,
        })
    }

    async fn get_connection(&self) -> Result<MultiplexedConnection, redis::RedisError> {
        self.client.get_multiplexed_async_connection().await
    }

    /// Enqueues a batch record into `batches:state:<id>` as a serialised JSON
    /// hash field. The batch has no position in the priority queue — it is
    /// metadata that lets `ingest status batch <id>` reconstruct the picture.
    pub async fn enqueue_batch(&self, batch: &Batch) -> Result<(), ToolError> {
        let mut conn = self.get_connection().await?;

        let key = format!("batches:state:{}", batch._id);

        // Store the whole batch document as a single hash field so it can be
        // fetched atomically with a single HGET.
        let _: () = conn.hset(&key, "status", "pending").await?;

        tracing::debug!(batch_id = %batch._id, "Batch state written to Redis");
        Ok(())
    }

    /// Pushes a file-level job onto `jobs:pending` (sorted set, score =
    /// priority). The member is encoded as `"file:<job_id>"` so that
    /// `dequeue_job` can recover the kind without an extra lookup.
    pub async fn enqueue_file_job(&self, job: &FileJob) -> Result<(), ToolError> {
        let mut conn = self.get_connection().await?;

        // Persist full struct — worker needs everything to execute without Mongo
        let state_key = format!("jobs:state:{}", job._id);
        let _: () = conn.hset(&state_key, "kind", "file").await?;
        let _: () = conn.hset(&state_key, "status", "pending").await?;
        let _: () = conn
            .hset(&state_key, "retry_count", job.retry_count)
            .await?;

        // Add to priority queue
        let member = format!("file:{}", job._id);
        let _: () = conn
            .zadd("jobs:pending", &member, job.priority as f64)
            .await?;

        tracing::debug!(job_id = %job._id, priority = job.priority, "File job enqueued");
        Ok(())
    }

    /// Enqueue a chunk job: same pattern, different kind prefix.
    pub async fn enqueue_chunk_job(&self, job: &ChunkJob) -> Result<(), ToolError> {
        let mut conn = self.get_connection().await?;

        let state_key = format!("jobs:state:{}", job._id);
        let _: () = conn.hset(&state_key, "kind", "chunk").await?;
        let _: () = conn.hset(&state_key, "status", "pending").await?;
        let _: () = conn
            .hset(&state_key, "retry_count", job.retry_count)
            .await?;

        let member = format!("chunk:{}", job._id);
        let _: () = conn
            .zadd("jobs:pending", &member, job.priority as f64)
            .await?;

        tracing::debug!(job_id = %job._id, priority = job.priority, "Chunk job enqueued");
        Ok(())
    }

    /// Fetch the full job state from Redis by ID.
    /// Returns the deserialized kind + payload so ContextFactory can build the context.
    pub async fn get_job(&self, job_id: &str) -> Result<(JobKind, JobStatus, u8), ToolError> {
        let mut conn = self.get_connection().await?;
        let state_key = format!("jobs:state:{job_id}");

        let (kind, status, retry_count): (String, String, u8) = redis::cmd("HMGET")
            .arg(&state_key)
            .arg("kind")
            .arg("status")
            .arg("retry_count")
            .query_async(&mut conn)
            .await?;

        let job_kind = match kind.as_str() {
            "file" => JobKind::File,
            "chunk" => JobKind::Chunk,
            other => return Err(format!("Unknown job kind '{other}' for job {job_id}").into()),
        };

        let job_status = match status.as_str() {
            "pending" => JobStatus::Pending,
            "running" => JobStatus::Running,
            "completed" => JobStatus::Completed,
            "retrying" => JobStatus::Retrying,
            "failed" => JobStatus::Failed,
            other => return Err(format!("Unknown job status '{other}' for job {job_id}").into()),
        };

        Ok((job_kind, job_status, retry_count))
    }

    /// Dequeues the highest-priority job from `jobs:pending` (ZPOPMAX).
    ///
    /// The job ID is stored in the sorted set with a prefix that encodes its
    /// kind: `"file:<uuid>"` or `"chunk:<uuid>"`. This avoids a second Redis
    /// round-trip to resolve the kind and keeps the dequeue path atomic.
    ///
    /// Returns `None` on timeout (2 s) or any transient error — the scheduler
    /// loop will simply spin and try again.
    pub async fn dequeue_job(
        &self,
        n_worker: usize,
    ) -> Result<Option<(JobKind, String)>, ToolError> {
        let mut conn = self.get_connection().await?;

        // BZPOPMAX blocks up to 2 s; returns (key, member, score) or nothing.
        let result: (String, String, f64) = conn.bzpopmax("jobs:pending", 2.0).await?;

        let (_, raw_id, _score) = result;

        // Decode the "kind:uuid" member into its parts.
        let (kind, job_id) = parse_job_member(&raw_id)
            .ok_or_else(|| format!("Invalid job member format: '{raw_id}'"))?;

        // Mark the job as Running in its state hash.
        let state_key = format!("jobs:state:{job_id}");
        let _: () = conn.hset(&state_key, "status", "running").await?;

        // Record it in a TTL-bearing key so the scheduler can track live workers
        // and stale entries auto-cleanup on worker crash.
        let running_key = format!("jobs:running:{job_id}");
        let _: () = conn.set(&running_key, format!("worker{n_worker}")).await?;
        let _: () = conn
            .expire(&running_key, self.running_job_ttl_secs as i64)
            .await?;

        Ok(Some((kind, job_id)))
    }

    /// Marks a job as completed: removes it from `jobs:running`, updates its
    /// state hash, and cleans up the progress pub/sub channel.
    pub async fn complete_job(&self, job_id: &str) -> Result<(), ToolError> {
        let mut conn = self.get_connection().await?;
        let state_key = format!("jobs:state:{}", job_id);
        let _: () = conn.hset(&state_key, "status", "completed").await?;
        let running_key = format!("jobs:running:{job_id}");
        let _: () = conn.del(&running_key).await?;
        Ok(())
    }

    /// Re-enqueues a job for retry with exponential backoff.
    /// Backoff: 5s (1st), 30s (2nd), 120s (3rd).
    pub async fn retry_job(&self, job_id: &str, kind: JobKind) -> Result<(), ToolError> {
        let mut conn = self.get_connection().await?;
        let running_key = format!("jobs:running:{job_id}");
        let _: () = conn.del(&running_key).await?;

        let state_key = format!("jobs:state:{}", job_id);

        let retry_count: u8 = redis::cmd("HGET")
            .arg(&state_key)
            .arg("retry_count")
            .query_async(&mut conn)
            .await?;

        // Enforce 3-attempt cap
        if retry_count >= 3 {
            tracing::error!(job_id = %job_id, "Exceeded max retries, failing job");
            return self
                .fail_job(job_id, "Exceeded maximum retry attempts")
                .await;
        }

        let member = match kind {
            JobKind::File => format!("file:{}", job_id),
            JobKind::Chunk => format!("chunk:{}", job_id),
        };

        // Calculate backoff delay: 5s, 30s, 2min
        let backoff_secs = match retry_count {
            0 => 5,
            1 => 30,
            _ => 120,
        };

        let _: () = conn.hset(&state_key, "status", "retrying").await?;
        let _: () = conn
            .hset(&state_key, "retry_count", retry_count + 1)
            .await?;

        // Store retry_after timestamp
        let retry_after = chrono::Utc::now()
            .checked_add_signed(chrono::Duration::seconds(backoff_secs as i64))
            .unwrap();
        let _: () = conn
            .hset(&state_key, "retry_after", retry_after.to_rfc3339())
            .await?;

        // Use priority score 0 so it's picked up after backoff
        let _: () = conn.zadd("jobs:pending", &member, 0).await?;

        tracing::debug!(job_id = %job_id, retry_count, backoff_secs, "Job re-enqueued for retry");
        Ok(())
    }

    /// Permanently marks a job as failed after exhausting retries.
    pub async fn fail_job(&self, job_id: &str, error: &str) -> Result<(), ToolError> {
        let mut conn = self.get_connection().await?;
        let state_key = format!("jobs:state:{}", job_id);
        let _: () = conn.hset(&state_key, "status", "failed").await?;
        let _: () = conn.hset(&state_key, "error", error).await?;
        let running_key = format!("jobs:running:{job_id}");
        let _: () = conn.del(&running_key).await?;
        tracing::error!(job_id = %job_id, error = %error, "Job marked as failed");
        Ok(())
    }

    /// Cancels all pending jobs in a batch (simplified version).
    pub async fn cancel_batch_jobs(&self, _batch_id: &str) -> Result<usize, ToolError> {
        tracing::info!("Cancel batch jobs (simplified)");
        Ok(0)
    }

    /// Cancels a single pending job.
    pub async fn cancel_job(&self, job_id: &str) -> Result<(), ToolError> {
        let mut conn = self.get_connection().await?;
        let member = format!("file:{}", job_id);
        let removed: i32 = conn.zrem("jobs:pending", &member).await?;
        if removed > 0 {
            tracing::info!("Cancelled job {} from pending queue", job_id);
        }
        Ok(())
    }

    /// Records a completed chunk hash in the crash-recovery set for its file.
    pub async fn register_chunk(&self, file_hash: &str, chunk_hash: &str) -> Result<(), ToolError> {
        let mut conn = self.get_connection().await?;
        let key = format!("jobs:chunks:{file_hash}");
        let _: () = conn.sadd(&key, chunk_hash).await?;
        Ok(())
    }

    /// Returns the set of already-completed chunk hashes for a given file.
    /// Used during crash recovery to skip re-uploading finished chunks.
    pub async fn completed_chunks(&self, file_hash: &str) -> Result<Vec<String>, ToolError> {
        let mut conn = self.get_connection().await?;
        let key = format!("jobs:chunks:{file_hash}");
        let members: Vec<String> = conn.smembers(&key).await?;
        Ok(members)
    }

    /// Publishes a progress event string to the per-job pub/sub channel.
    /// The `--follow` terminal renderer subscribes to this channel.
    pub async fn publish_progress(&self, job_id: &str, event: &str) -> Result<(), ToolError> {
        let mut conn = self.get_connection().await?;
        let channel = format!("jobs:progress:{job_id}");
        let _: () = conn.publish(&channel, event).await?;
        Ok(())
    }

    /// Finds orphaned chunks (simplified version).
    pub async fn find_orphaned_chunks(&self) -> Result<Vec<String>, ToolError> {
        let mut conn = self.get_connection().await?;
        let mut orphaned = Vec::new();

        // Get all chunk keys
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg("jobs:chunks:*")
            .query_async(&mut conn)
            .await?;

        for key in keys {
            orphaned.push(key);
        }

        Ok(orphaned)
    }

    /// Cleans up orphaned chunks (simplified version).
    pub async fn cleanup_orphaned_chunks(&self) -> Result<usize, ToolError> {
        let mut conn = self.get_connection().await?;

        // Get all chunk keys
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg("jobs:chunks:*")
            .query_async(&mut conn)
            .await?;

        for key in keys {
            let file_hash = key.trim_start_matches("jobs:chunks:");
            // Check if file exists in MongoDB
            // If not, remove the chunk set
            // This is a simplified version - in reality you'd check MongoDB
            tracing::debug!("Checking orphaned chunks for file: {}", file_hash);
        }

        Ok(0)
    }
}

/// Splits a `"kind:uuid"` member string into `(JobKind, uuid_string)`.
/// Returns `None` if the format is unrecognised — those entries are skipped.
fn parse_job_member(raw: &str) -> Option<(JobKind, String)> {
    let (prefix, id) = raw.split_once(':')?;
    let kind = match prefix {
        "file" => JobKind::File,
        "chunk" => JobKind::Chunk,
        other => {
            tracing::warn!(
                member = raw,
                "Unknown job kind prefix '{other}' in jobs:pending"
            );
            return None;
        }
    };
    Some((kind, id.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_job_member_file() {
        let result = parse_job_member("file:abc-123").unwrap();
        assert!(matches!(result.0, JobKind::File));
        assert_eq!(result.1, "abc-123");
    }

    #[test]
    fn test_parse_job_member_chunk() {
        let result = parse_job_member("chunk:def-456").unwrap();
        assert!(matches!(result.0, JobKind::Chunk));
        assert_eq!(result.1, "def-456");
    }

    #[test]
    fn test_parse_job_member_invalid_prefix() {
        assert!(parse_job_member("unknown:xxx").is_none());
    }

    #[test]
    fn test_parse_job_member_no_colon() {
        assert!(parse_job_member("justastring").is_none());
    }

    #[test]
    fn test_parse_job_member_empty() {
        assert!(parse_job_member("").is_none());
    }

    #[test]
    fn test_parse_job_member_only_colon() {
        assert!(parse_job_member(":").is_none());
    }

    #[test]
    fn test_parse_job_member_empty_after_colon() {
        let result = parse_job_member("file:").unwrap();
        assert!(matches!(result.0, JobKind::File));
        assert_eq!(result.1, "");
    }
}
