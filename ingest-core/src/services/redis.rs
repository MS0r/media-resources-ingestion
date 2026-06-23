use crate::{
    error::ToolError,
    handlers::jobs::{Batch, ChunkJob, FileJob, JobKind},
    models::{ChunkRef, JobStatusFilter, ProgressEvent, ProgressJobType, ProgressStatus},
    services::mongo::MongoService,
};
use redis::{AsyncCommands, Client, aio::MultiplexedConnection};

#[derive(Clone)]
pub struct RedisService {
    client: Client,
    running_job_ttl_secs: u64,
    max_retries: u8,
    backoff_secs: Vec<u64>,
}

impl RedisService {
    pub fn new(
        redis_uri: &str,
        running_job_ttl_secs: u64,
        max_retries: u8,
        backoff_secs: Vec<u64>,
    ) -> Result<Self, redis::RedisError> {
        let client = Client::open(redis_uri)?;
        Ok(Self {
            client,
            running_job_ttl_secs,
            max_retries,
            backoff_secs,
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
    pub async fn get_job(&self, job_id: &str) -> Result<(JobKind, JobStatusFilter, u8), ToolError> {
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
            "pending" => JobStatusFilter::Pending,
            "running" => JobStatusFilter::Running,
            "completed" => JobStatusFilter::Completed,
            "retrying" => JobStatusFilter::Retrying,
            "failed" => JobStatusFilter::Failed,
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

    /// Re-enqueues a job for retry with configurable backoff.
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

        if retry_count >= self.max_retries {
            tracing::error!(job_id = %job_id, max_retries = self.max_retries, "Exceeded max retries, failing job");
            return self
                .fail_job(
                    job_id,
                    &format!("Exceeded maximum retry attempts ({})", self.max_retries),
                )
                .await;
        }

        let member = match kind {
            JobKind::File => format!("file:{}", job_id),
            JobKind::Chunk => format!("chunk:{}", job_id),
        };

        let backoff_secs = self
            .backoff_secs
            .get(retry_count as usize)
            .copied()
            .unwrap_or_else(|| *self.backoff_secs.last().unwrap_or(&120));

        let _: () = conn.hset(&state_key, "status", "retrying").await?;
        let _: () = conn
            .hset(&state_key, "retry_count", retry_count + 1)
            .await?;

        // Store retry_after timestamp
        let retry_after = chrono::Utc::now()
            .checked_add_signed(chrono::Duration::seconds(backoff_secs as i64))
            .ok_or_else(|| ToolError::Message("Timestamp overflow in retry_after".into()))?;
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

    /// Refreshes the TTL on the running lease so long-running jobs don't
    /// get their lease stolen by another worker.
    pub async fn renew_lease(&self, job_id: &str) -> Result<(), ToolError> {
        let mut conn = self.get_connection().await?;
        let running_key = format!("jobs:running:{job_id}");
        let _: () = conn
            .expire(&running_key, self.running_job_ttl_secs as i64)
            .await?;
        Ok(())
    }

    /// Scans for orphaned `jobs:running:*` keys (stale from a crashed worker)
    /// and re-enqueues them as pending so a healthy worker can pick them up.
    /// Called once at worker startup.
    pub async fn recover_orphaned_jobs(&self) -> Result<usize, ToolError> {
        let mut conn = self.get_connection().await?;

        let running_keys: Vec<String> = conn.keys("jobs:running:*").await?;

        let mut recovered = 0usize;
        for key in running_keys {
            let job_id = key
                .strip_prefix("jobs:running:")
                .unwrap_or(&key)
                .to_string();

            let state_key = format!("jobs:state:{job_id}");

            let kind: Option<String> = conn.hget(&state_key, "kind").await?;

            let member = match kind.as_deref() {
                Some("file") => format!("file:{job_id}"),
                Some("chunk") => format!("chunk:{job_id}"),
                _ => {
                    tracing::warn!(job_id = %job_id, "Orphaned running key with unknown kind, deleting");
                    let _: () = conn.del(&key).await?;
                    continue;
                }
            };

            let _: () = conn.hset(&state_key, "status", "pending").await?;
            let _: () = conn.del(&key).await?;
            let _: () = conn.zadd("jobs:pending", &member, 0.0).await?;
            recovered += 1;
            tracing::info!(job_id = %job_id, kind = ?kind, "Recovered orphaned job");
        }

        if recovered > 0 {
            tracing::warn!(
                count = recovered,
                "Recovered orphaned jobs from crashed workers"
            );
        }
        Ok(recovered)
    }

    /// Cancels all pending jobs in a batch by removing their IDs from the
    /// Redis sorted set. Accepts the list of job IDs (from the Batch document
    /// in Mongo).
    pub async fn cancel_batch_jobs(&self, job_ids: &[String]) -> Result<usize, ToolError> {
        let mut conn = self.get_connection().await?;
        let mut removed = 0usize;
        for job_id in job_ids {
            for prefix in &["file", "chunk"] {
                let member = format!("{}:{}", prefix, job_id);
                let n: usize = conn.zrem("jobs:pending", &member).await?;
                removed += n;
            }
        }
        tracing::info!(count = removed, "Cancelled jobs from Redis pending queue");
        Ok(removed)
    }

    /// Cancels a list of specific job IDs from the pending queue.
    pub async fn cancel_jobs(&self, job_ids: &[String]) -> Result<usize, ToolError> {
        let mut conn = self.get_connection().await?;
        let mut removed = 0usize;
        for job_id in job_ids {
            for prefix in &["file", "chunk"] {
                let member = format!("{}:{}", prefix, job_id);
                let n: usize = conn.zrem("jobs:pending", &member).await?;
                removed += n;
            }
        }
        Ok(removed)
    }

    /// Cancels a single pending job (handles both file and chunk prefixes).
    pub async fn cancel_job(&self, job_id: &str) -> Result<(), ToolError> {
        let mut conn = self.get_connection().await?;
        let mut removed: usize = 0;
        for prefix in &["file", "chunk"] {
            let member = format!("{}:{}", prefix, job_id);
            let n: usize = conn.zrem("jobs:pending", &member).await?;
            removed += n;
        }
        if removed > 0 {
            tracing::info!("Cancelled job {} from pending queue", job_id);
        }
        Ok(())
    }

    /// Create a counter for the counter pattern following the given parent_id
    pub async fn create_counter(&self, parent_id: &str) -> Result<(), ToolError> {
        let mut conn = self.get_connection().await?;
        let key = format!("jobs:counter:{parent_id}");
        let _: () = conn.set(&key, "0").await?;
        Ok(())
    }

    /// Records a completed chunk hash in the crash-recovery set for its file.
    pub async fn register_chunk(&self, parent_id: &str, chunk_id: &str) -> Result<(), ToolError> {
        let mut conn = self.get_connection().await?;
        let key = format!("jobs:chunks:{parent_id}");
        let _: () = conn.sadd(&key, chunk_id).await?;
        Ok(())
    }

    /// Returns the set of already-completed chunk hashes for a given file.
    /// Used during crash recovery to skip re-uploading finished chunks.
    pub async fn completed_chunks(&self, parent_id: &str) -> Result<Vec<String>, ToolError> {
        let mut conn = self.get_connection().await?;
        let key = format!("jobs:chunks:{parent_id}");
        let members: Vec<String> = conn.smembers(&key).await?;
        Ok(members)
    }

    /// Store a chunk's metadata in a Redis hash for later manifest assembly.
    /// Key: `jobs:chunk_results:<file_hash>`, field: chunk_index, value: JSON.
    pub async fn complete_chunk(
        &self,
        _chunk_id: &str,
        chunk_ref: ChunkRef,
        chunk_index: u32,
        parent_id: &str,
    ) -> Result<u32, ToolError> {
        let mut conn = self.get_connection().await?;
        let result_key = format!("jobs:chunk_results:{parent_id}");
        let count_key = format!("jobs:counter:{parent_id}");
        let json = serde_json::to_string(&chunk_ref)?;

        let (count,): (u32,) = redis::pipe()
            .atomic()
            .hset(&result_key, chunk_index, json)
            .ignore()
            .incr(&count_key, 1)
            .query_async(&mut conn)
            .await?;

        Ok(count)
    }

    /// Fetch all chunk results for a file. Returns `Vec<(chunk_index, ChunkRef)>`.
    pub async fn get_all_chunk_results(
        &self,
        parent_id: &str,
    ) -> Result<Vec<(u32, ChunkRef)>, ToolError> {
        let mut conn = self.get_connection().await?;
        let key = format!("jobs:chunk_results:{parent_id}");
        let entries: Vec<(String, String)> = conn.hgetall(&key).await?;
        let mut results = Vec::with_capacity(entries.len());
        for (idx_str, json) in entries {
            let index: u32 = idx_str
                .parse()
                .map_err(|e| ToolError::Message(format!("Invalid chunk index '{idx_str}': {e}")))?;
            let chunk_ref: ChunkRef = serde_json::from_str(&json)?;
            results.push((index, chunk_ref));
        }
        Ok(results)
    }

    /// Remove all chunk result data for a file. Called after finalization.
    pub async fn cleanup_chunk_results(&self, parent_id: &str) -> Result<(), ToolError> {
        let mut conn = self.get_connection().await?;
        let key = format!("jobs:chunk_results:{parent_id}");
        let _: () = conn.del(&key).await?;
        let chunks_key = format!("jobs:chunks:{parent_id}");
        let _: () = conn.del(&chunks_key).await?;
        Ok(())
    }

    /// Finds orphaned chunks — chunk tracking keys whose file no longer exists
    /// in MongoDB. Returns the list of orphaned file hashes.
    pub async fn find_orphaned_chunks(&self) -> Result<Vec<String>, ToolError> {
        let mut conn = self.get_connection().await?;

        let keys: Vec<String> = redis::cmd("KEYS")
            .arg("jobs:chunks:*")
            .query_async(&mut conn)
            .await?;

        let mut orphaned = Vec::new();
        for key in keys {
            let file_hash = key.strip_prefix("jobs:chunks:").unwrap_or(&key).to_string();
            orphaned.push(file_hash);
        }

        Ok(orphaned)
    }

    /// Cleans up orphaned chunk tracking keys by cross-referencing with
    /// MongoService. Removes Redis entries for files that no longer exist
    /// in MongoDB. Returns the number of cleaned keys.
    pub async fn cleanup_orphaned_chunks(&self, mongo: &MongoService) -> Result<usize, ToolError> {
        let orphans = self.find_orphaned_chunks().await?;
        let mut conn = self.get_connection().await?;
        let mut cleaned = 0usize;

        for file_hash in &orphans {
            match mongo.get_file_job(file_hash).await {
                Ok(Some(_)) => {}
                Ok(None) => {
                    let key = format!("jobs:chunks:{file_hash}");
                    let _: () = conn.del(&key).await?;
                    cleaned += 1;
                    tracing::info!(file_hash = %file_hash, "Cleaned up orphaned chunk tracking key");
                }
                Err(e) => {
                    tracing::warn!(file_hash = %file_hash, error = %e, "Could not check file existence")
                }
            }
        }

        Ok(cleaned)
    }

    /// Publish a progress event to the job's progress channel.
    /// Idempotent: if no subscriber is listening, the PUBLISH is a no-op.
    pub async fn publish_progress(
        &self,
        job_id: &str,
        event: &ProgressEvent,
    ) -> Result<(), ToolError> {
        let mut conn = self.get_connection().await?;
        let channel = format!("jobs:progress:{job_id}");
        let payload = serde_json::to_string(event)?;
        let _: () = conn.publish(channel, payload).await?;
        Ok(())
    }
}

/// Lightweight handle for publishing progress events from a job handler.
/// Clonable and cheap — holds only a job_id and a clone of `Arc<RedisService>`.
#[derive(Clone)]
pub struct ProgressReporter {
    job_id: String,
    redis: RedisService,
}

impl ProgressReporter {
    pub fn new(job_id: String, redis: RedisService) -> Self {
        Self { job_id, redis }
    }

    pub async fn report(
        &self,
        stage: &str,
        current: u32,
        total: Option<u32>,
        message: Option<&str>,
    ) {
        self.publish(ProgressStatus::Running, stage, current, total, message)
            .await;
    }

    pub async fn done(&self, message: Option<&str>) {
        self.publish(ProgressStatus::Done, "done", 1, Some(1), message)
            .await;
    }

    pub async fn fail(&self, reason: &str) {
        self.publish(ProgressStatus::Failed, "failed", 0, None, Some(reason))
            .await;
    }

    async fn publish(
        &self,
        status: ProgressStatus,
        stage: &str,
        current: u32,
        total: Option<u32>,
        message: Option<&str>,
    ) {
        let event = ProgressEvent {
            job_id: self.job_id.clone(),
            job_type: ProgressJobType::FileJob,
            stage: stage.to_string(),
            current,
            total,
            status,
            message: message.map(|s| s.to_string()),
        };
        if let Err(e) = self.redis.publish_progress(&self.job_id, &event).await {
            tracing::warn!(job_id = %self.job_id, error = %e, "Failed to publish progress");
        }
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
