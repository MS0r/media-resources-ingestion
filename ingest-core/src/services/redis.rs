use crate::{
    error::ToolError,
    handlers::jobs::{Batch, ChunkJob, FileJob, JobKind},
    models::{ChunkRef, JobStatusFilter, ProgressEvent, ProgressJobType, ProgressStatus},
    services::mongo::MongoService,
};
use crc32fast::Hasher;
use futures_util::StreamExt;
use redis::{AsyncCommands, Client, aio::MultiplexedConnection};
use std::time::Duration;

const MAX_REDIS_RETRIES: usize = 3;
const REDIS_BACKOFF_BASE_MS: u64 = 100;

/// Retry a Redis operation with exponential backoff.
/// Used to absorb transient connection drops / network blips.
async fn with_retry<F, Fut, T>(op_name: &'static str, mut f: F) -> Result<T, redis::RedisError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, redis::RedisError>>,
{
    let mut attempt = 0;
    loop {
        match f().await {
            Ok(v) => {
                if attempt > 0 {
                    tracing::debug!(op = op_name, attempt, "Redis op succeeded after retry");
                }
                return Ok(v);
            }
            Err(e) if attempt < MAX_REDIS_RETRIES => {
                let backoff_ms = REDIS_BACKOFF_BASE_MS * (1 << attempt);
                tracing::warn!(
                    op = op_name,
                    attempt,
                    backoff_ms,
                    error = %e,
                    "Redis op failed, retrying"
                );
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                attempt += 1;
            }
            Err(e) => {
                tracing::error!(op = op_name, error = %e, "Redis op failed after retries");
                return Err(e);
            }
        }
    }
}

pub fn compute_shard(key: &str, shard_count: u32) -> u32 {
    let mut h = Hasher::new();
    h.update(key.as_bytes());
    h.finalize() % shard_count
}

pub fn shard_key(shard: u32) -> String {
    format!("jobs:pending:{{{shard}}}")
}

pub fn derive_worker_id() -> u32 {
    if let Ok(s) = std::env::var("INGEST_WORKER_ID") {
        if let Ok(n) = s.parse::<u32>() {
            return n;
        }
    }
    let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown-host".to_string());
    let pid = std::process::id();
    let mut h = Hasher::new();
    h.update(hostname.as_bytes());
    h.update(&pid.to_le_bytes());
    h.finalize()
}

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
        with_retry("get_connection", || {
            self.client.get_multiplexed_async_connection()
        })
        .await
    }

    /// Flush the entire Redis database. **Test-only helper** — do not call in
    /// production code.
    pub async fn flush_db(&self) -> Result<(), ToolError> {
        let mut conn = self.get_connection().await?;
        let _: String = redis::cmd("FLUSHDB").query_async(&mut conn).await?;
        Ok(())
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
    /// Shard is computed from `job._id`.
    pub async fn enqueue_file_job(&self, job: &FileJob, shard_count: u32) -> Result<(), ToolError> {
        let mut conn = self.get_connection().await?;

        // Persist full struct — worker needs everything to execute without Mongo
        let state_key = format!("jobs:state:{}", job._id);
        let _: () = conn.hset(&state_key, "kind", "file").await?;
        let _: () = conn.hset(&state_key, "status", "pending").await?;
        let _: () = conn
            .hset(&state_key, "retry_count", job.retry_count)
            .await?;

        // Add to priority queue — sharded by job_id
        let shard = compute_shard(&job._id, shard_count);
        let member = format!("file:{}", job._id);
        let _: () = conn
            .zadd(shard_key(shard), &member, job.priority as f64)
            .await?;

        tracing::debug!(job_id = %job._id, priority = job.priority, shard, "File job enqueued");
        Ok(())
    }

    /// Enqueue a chunk job: same pattern, different kind prefix.
    /// Shard is computed from `job.parent_job_id` for intra-file locality.
    pub async fn enqueue_chunk_job(
        &self,
        job: &ChunkJob,
        shard_count: u32,
    ) -> Result<(), ToolError> {
        let mut conn = self.get_connection().await?;

        let state_key = format!("jobs:state:{}", job._id);
        let _: () = conn.hset(&state_key, "kind", "chunk").await?;
        let _: () = conn.hset(&state_key, "status", "pending").await?;
        let _: () = conn
            .hset(&state_key, "retry_count", job.retry_count)
            .await?;
        let _: () = conn
            .hset(&state_key, "parent_job_id", &job.parent_job_id)
            .await?;

        // Shard by parent_job_id so all chunks from the same file land on one shard
        let shard = compute_shard(&job.parent_job_id, shard_count);
        let member = format!("chunk:{}", job._id);
        let _: () = conn
            .zadd(shard_key(shard), &member, job.priority as f64)
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

    /// Dequeues the highest-priority job from across all sharded
    /// `jobs:pending:{N}` sets (variadic BZPOPMAX over every shard).
    ///
    /// The job ID is stored in the sorted set with a prefix that encodes its
    /// kind: `"file:<uuid>"` or `"chunk:<uuid>"`. This avoids a second Redis
    /// round-trip to resolve the kind and keeps the dequeue path atomic.
    ///
    /// Returns `None` on timeout (2 s) or any transient error — the scheduler
    /// loop will simply spin and try again.
    pub async fn dequeue_job(
        &self,
        worker_id: u32,
        shard_count: u32,
    ) -> Result<Option<(JobKind, String)>, ToolError> {
        let mut conn = self.get_connection().await?;

        let keys: Vec<String> = (0..shard_count).map(shard_key).collect();

        // BZPOPMAX blocks up to 2 s; returns (key, member, score) or nil.
        let result: Option<(String, String, f64)> = redis::cmd("BZPOPMAX")
            .arg(&keys)
            .arg(2.0)
            .query_async(&mut conn)
            .await?;

        let Some((_key, raw_id, _score)) = result else {
            return Ok(None);
        };

        // Decode the "kind:uuid" member into its parts.
        let (kind, job_id) = parse_job_member(&raw_id)
            .ok_or_else(|| format!("Invalid job member format: '{raw_id}'"))?;

        // Mark the job as Running in its state hash.
        let state_key = format!("jobs:state:{job_id}");
        let _: () = conn.hset(&state_key, "status", "running").await?;

        // Record it in a TTL-bearing key so the scheduler can track live workers
        // and stale entries auto-cleanup on worker crash.
        let running_key = format!("jobs:running:{job_id}");
        let _: () = conn.set(&running_key, format!("worker{worker_id}")).await?;
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

    /// Read the `parent_job_id` from a chunk's state hash. Used by the
    /// scheduler to re-enqueue a chunk after a context-build miss without
    /// needing the caller to retain the parent_job_id.
    pub async fn get_parent_for_chunk(&self, job_id: &str) -> Result<String, ToolError> {
        let mut conn = self.get_connection().await?;
        let state_key = format!("jobs:state:{job_id}");
        let parent_id: String = redis::cmd("HGET")
            .arg(&state_key)
            .arg("parent_job_id")
            .query_async(&mut conn)
            .await?;
        Ok(parent_id)
    }

    /// Re-enqueue a chunk job back to `jobs:pending:{shard}` after a
    /// transient context-build failure (e.g. Mongo write lag).
    ///
    /// Clears the `jobs:running` lease, resets the state hash to
    /// `pending`, and pushes the member back to the shard's sorted set.
    /// Does NOT increment `retry_count` — this is a visibility recovery,
    /// not a retry of the job itself.
    pub async fn requeue_chunk_for_retry(
        &self,
        job_id: &str,
        parent_id: &str,
        shard_count: u32,
    ) -> Result<(), ToolError> {
        let mut conn = self.get_connection().await?;
        let state_key = format!("jobs:state:{job_id}");
        let running_key = format!("jobs:running:{job_id}");
        // Clear the running lease so recover_orphaned_jobs won't pick it up
        // and so dequeue_job won't see a stale lease.
        let _: () = conn.del(&running_key).await?;
        // Reset status to pending (the job is back in the queue).
        let _: () = conn.hset(&state_key, "status", "pending").await?;
        // Re-enqueue to the correct shard.
        let shard = compute_shard(parent_id, shard_count);
        let member = format!("chunk:{job_id}");
        let _: () = conn.zadd(shard_key(shard), &member, 0_f64).await?;
        tracing::info!(
            job_id = %job_id, shard,
            "Chunk re-enqueued for context-build recovery"
        );
        Ok(())
    }

    /// Re-enqueues a job for retry with configurable backoff.
    /// For chunks, `parent_job_id` is needed to compute the correct shard.
    pub async fn retry_job(
        &self,
        job_id: &str,
        kind: JobKind,
        shard_count: u32,
        parent_job_id: Option<&str>,
    ) -> Result<(), ToolError> {
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
        let shard = match kind {
            JobKind::File => compute_shard(job_id, shard_count),
            JobKind::Chunk => compute_shard(parent_job_id.unwrap_or(job_id), shard_count),
        };
        let _: () = conn.zadd(shard_key(shard), &member, 0).await?;

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
    pub async fn recover_orphaned_jobs(&self, shard_count: u32) -> Result<usize, ToolError> {
        let mut conn = self.get_connection().await?;

        let keys: Vec<String> = {
            let iter = conn.scan_match("jobs:running:*").await.map_err(|e| {
                tracing::error!(error = %e, "Failed to scan for orphaned jobs");
                ToolError::from(e)
            })?;
            let res: Vec<Result<String, _>> = iter.collect().await;
            res.into_iter().filter_map(|r| r.ok()).collect()
        };

        let mut rec = 0usize;
        for key in keys {
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

            let shard = compute_shard(&job_id, shard_count);
            let _: () = conn.hset(&state_key, "status", "pending").await?;
            let _: () = conn.del(&key).await?;
            let _: () = conn.zadd(shard_key(shard), &member, 0.0).await?;
            rec += 1;
            tracing::info!(job_id = %job_id, kind = ?kind, shard, "Recovered orphaned job");
        }

        if rec > 0 {
            tracing::warn!(count = rec, "Recovered orphaned jobs from crashed workers");
        }

        Ok(rec)
    }

    /// Delete all `jobs:running:*` keys. Called at worker shutdown
    /// so the next worker's `recover_orphaned_jobs` picks them up
    /// immediately instead of waiting for the 3600s TTL.
    pub async fn delete_all_running(&self) -> Result<usize, ToolError> {
        let mut conn = self.get_connection().await?;
        let keys: Vec<String> = {
            let iter = conn.scan_match("jobs:running:*").await.map_err(|e| {
                tracing::error!(error = %e, "Failed to scan for running keys");
                ToolError::from(e)
            })?;
            let res: Vec<Result<String, _>> = iter.collect().await;
            res.into_iter().filter_map(|r| r.ok()).collect()
        };
        let n = keys.len();
        if n > 0 {
            let _: () = conn.del(&keys).await?;
            tracing::info!(count = n, "Cleared running keys on shutdown");
        }
        Ok(n)
    }

    /// Cancels all pending jobs in a batch by removing their IDs from the
    /// Redis sorted set. Accepts the list of job IDs (from the Batch document
    /// in Mongo). Tries all shards since we don't know which shard holds each job.
    /// Cancels all pending jobs in a batch by removing their IDs from the
    /// Redis sorted set. Accepts the list of job IDs (from the Batch document
    /// in Mongo). Tries all shards since we don't know which shard holds each job.
    /// Batches into a single pipeline round-trip.
    pub async fn cancel_batch_jobs(
        &self,
        job_ids: &[String],
        shard_count: u32,
    ) -> Result<usize, ToolError> {
        let mut conn = self.get_connection().await?;
        let mut pipe = redis::pipe();
        for job_id in job_ids {
            for prefix in &["file", "chunk"] {
                let member = format!("{}:{}", prefix, job_id);
                for s in 0..shard_count {
                    pipe.zrem(shard_key(s), &member).ignore();
                }
            }
        }
        let results: Vec<i64> = pipe.query_async(&mut conn).await?;
        let removed = results.iter().filter(|&&n| n > 0).count();
        tracing::info!(count = removed, "Cancelled jobs from Redis pending queue");
        Ok(removed)
    }

    /// Cancels a single pending job (handles both file and chunk prefixes).
    /// Tries all shards since we don't know which shard holds the job.
    /// Uses a pipeline for a single round-trip.
    pub async fn cancel_job(&self, job_id: &str, shard_count: u32) -> Result<(), ToolError> {
        let mut conn = self.get_connection().await?;
        let mut pipe = redis::pipe();
        for prefix in &["file", "chunk"] {
            let member = format!("{}:{}", prefix, job_id);
            for s in 0..shard_count {
                pipe.zrem(shard_key(s), &member).ignore();
            }
        }
        let results: Vec<i64> = pipe.query_async(&mut conn).await?;
        let removed: usize = results.iter().filter(|&&n| n > 0).count();
        if removed > 0 {
            tracing::info!(job_id = %job_id, "Cancelled job from pending queue");
        }
        Ok(())
    }

    /// Move all members of `jobs:delayed` whose retry time has elapsed
    /// into the appropriate `jobs:pending:{shard}` sorted set.
    /// Called once per scheduler iteration before dequeue.
    pub async fn promote_due_delayed(&self, shard_count: u32) -> Result<usize, ToolError> {
        let mut conn = self.get_connection().await?;
        let now_ts = chrono::Utc::now().timestamp();

        // Upper bound: (now_ts + 1) * 1000 + shard_count covers all possible
        // encoded scores for jobs that are due now or earlier.
        let max_score = ((now_ts + 1) as f64) * 1000.0 + (shard_count as f64);
        let due: Vec<(String, f64)> = redis::cmd("ZRANGEBYSCORE")
            .arg("jobs:delayed")
            .arg("-inf")
            .arg(max_score)
            .arg("WITHSCORES")
            .query_async(&mut conn)
            .await?;

        if due.is_empty() {
            return Ok(0);
        }

        let mut promoted = 0;
        for (member, score) in &due {
            let retry_at_ts = (score / 1000.0).floor() as i64;
            if retry_at_ts > now_ts {
                continue;
            }

            let shard = score.fract().round() as u32;
            let shard = shard.min(shard_count.saturating_sub(1));

            let _: () = conn.zadd(shard_key(shard), member, 0_f64).await?;
            let _: () = conn.zrem("jobs:delayed", member).await?;
            promoted += 1;
        }

        if promoted > 0 {
            tracing::debug!(count = promoted, "Promoted delayed jobs to pending");
        }
        Ok(promoted)
    }

    /// Create a counter for the counter pattern following the given parent_id
    pub async fn create_counter(&self, parent_id: &str) -> Result<(), ToolError> {
        let mut conn = self.get_connection().await?;
        let key = format!("jobs:counter:{parent_id}");
        let _: () = conn.set(&key, "0").await?;
        let _: () = conn.expire(&key, (7 * 24 * 3600) as i64).await?;
        Ok(())
    }

    /// Records a completed chunk hash in the crash-recovery set for its file.
    pub async fn register_chunk(&self, parent_id: &str, chunk_id: &str) -> Result<(), ToolError> {
        let mut conn = self.get_connection().await?;
        let key = format!("jobs:chunks:{parent_id}");
        let _: () = conn.sadd(&key, chunk_id).await?;
        let _: () = conn.expire(&key, (7 * 24 * 3600) as i64).await?;
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

    /// Returns the number of chunk jobs for a parent that are NOT yet
    /// in a terminal state (completed/failed/cancelled).
    /// Uses SCAN over jobs:state:* keys to find chunk entries with the given parent.
    pub async fn live_chunk_count(&self, parent_id: &str) -> Result<u32, ToolError> {
        let mut conn = self.get_connection().await?;
        let keys: Vec<String> = {
            let iter = conn.scan_match("jobs:state:*").await.map_err(|e| {
                tracing::error!(error = %e, "Failed to scan for chunk state keys");
                ToolError::from(e)
            })?;
            let res: Vec<Result<String, _>> = iter.collect().await;
            res.into_iter().filter_map(|r| r.ok()).collect()
        };

        let mut count = 0u32;
        for state_key in keys {
            let (kind, status, p_id): (Option<String>, Option<String>, Option<String>) =
                redis::cmd("HMGET")
                    .arg(&state_key)
                    .arg("kind")
                    .arg("status")
                    .arg("parent_job_id")
                    .query_async(&mut conn)
                    .await?;
            if kind.as_deref() == Some("chunk")
                && p_id.as_deref() == Some(parent_id)
                && !matches!(
                    status.as_deref(),
                    Some("completed") | Some("failed") | Some("cancelled")
                )
            {
                count += 1;
            }
        }
        Ok(count)
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

        // Auto-expire chunk tracking keys after 7 days
        let ttl_secs = 7 * 24 * 3600;
        let _: () = conn.expire(&result_key, ttl_secs).await?;
        let _: () = conn.expire(&count_key, ttl_secs).await?;

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
