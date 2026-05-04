use crate::{
    cli::JobStatus,
    error::BoxedError,
    handlers::jobs::{Batch, ChunkJob, FileJob, JobKind},
};
use redis::{AsyncCommands, Client, aio::MultiplexedConnection};

#[derive(Clone)]
pub struct RedisService {
    client: Client,
}

impl RedisService {
    pub fn new(redis_url: &str) -> Result<Self, redis::RedisError> {
        let client = Client::open(redis_url)?;
        Ok(Self { client })
    }

    async fn get_connection(&self) -> Result<MultiplexedConnection, redis::RedisError> {
        self.client.get_multiplexed_async_connection().await
    }

    /// Enqueues a batch record into `batches:state:<id>` as a serialised JSON
    /// hash field. The batch has no position in the priority queue — it is
    /// metadata that lets `ingest status batch <id>` reconstruct the picture.
    pub async fn enqueue_batch(&self, batch: &Batch) -> Result<(), BoxedError> {
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
    pub async fn enqueue_file_job(&self, job: &FileJob) -> Result<(), BoxedError> {
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
    pub async fn enqueue_chunk_job(&self, job: &ChunkJob) -> Result<(), BoxedError> {
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
    pub async fn get_job(&self, job_id: &str) -> Result<(JobKind, JobStatus, u8), BoxedError> {
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
    pub async fn dequeue_job(&self) -> Result<Option<(JobKind, String)>, BoxedError> {
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

        // Record it in jobs:running so the scheduler can track live workers.
        let _: () = conn.hset("jobs:running", &job_id, "worker").await?;

        Ok(Some((kind, job_id)))
    }

    /// Marks a job as completed: removes it from `jobs:running`, updates its
    /// state hash, and cleans up the progress pub/sub channel.
    pub async fn complete_job(&self, job_id: &str) -> Result<(), BoxedError> {
        todo!()
    }

    /// Re-enqueues a job for retry with the same priority score. The caller is
    /// responsible for enforcing the 3-attempt cap before calling this — if the
    /// cap is exceeded, call `fail_job` instead.
    pub async fn retry_job(&self, job_id: &str, kind: JobKind) -> Result<(), BoxedError> {
        let mut conn = self.get_connection().await?;
        let _: () = conn.hdel("jobs:running", job_id).await?;

        let state_key = format!("jobs:state:{}", job_id);
        // Add back to pending queue
        let member = match kind {
            JobKind::File => format!("file:{}", job_id),
            JobKind::Chunk => format!("chunk:{}", job_id),
        };

        let retry_count: u8 = redis::cmd("HMGET")
            .arg(&state_key)
            .arg("retry_count")
            .query_async(&mut conn)
            .await?;

        let _: () = conn.hset(&state_key, "status", "retrying").await?;
        let _: () = conn
            .hset(&state_key, "retry_count", retry_count+1)
            .await?;

        let _: () = conn.zadd("jobs:pending", &member, 1).await?;

        tracing::debug!(job_id = %job_id, retry_count, "Job re-enqueued for retry");
        Ok(())
    }

    /// Permanently marks a job as failed after exhausting retries.
    pub async fn fail_job(&self, job_id: &str, error: &str) -> Result<(), BoxedError> {
        let mut conn = self.get_connection().await?;
        let state_key = format!("jobs:state:{}", job_id);
        let _: () = conn.hset(&state_key, "status", "failed").await?;
        let _: () = conn.hset(&state_key, "error", error).await?;
        let _: () = conn.hdel("jobs:running", job_id).await?;
        tracing::error!(job_id = %job_id, error = %error, "Job marked as failed");
        Ok(())
    }

    /// Records a completed chunk hash in the crash-recovery set for its file.
    pub async fn register_chunk(
        &self,
        file_hash: &str,
        chunk_hash: &str,
    ) -> Result<(), BoxedError> {
        let mut conn = self.get_connection().await?;
        let key = format!("jobs:chunks:{file_hash}");
        let _: () = conn.sadd(&key, chunk_hash).await?;
        Ok(())
    }

    /// Returns the set of already-completed chunk hashes for a given file.
    /// Used during crash recovery to skip re-uploading finished chunks.
    pub async fn completed_chunks(&self, file_hash: &str) -> Result<Vec<String>, BoxedError> {
        let mut conn = self.get_connection().await?;
        let key = format!("jobs:chunks:{file_hash}");
        let members: Vec<String> = conn.smembers(&key).await?;
        Ok(members)
    }

    /// Publishes a progress event string to the per-job pub/sub channel.
    /// The `--follow` terminal renderer subscribes to this channel.
    pub async fn publish_progress(&self, job_id: &str, event: &str) -> Result<(), BoxedError> {
        let mut conn = self.get_connection().await?;
        let channel = format!("jobs:progress:{job_id}");
        let _: () = conn.publish(&channel, event).await?;
        Ok(())
    }

    async fn write_job_state(
        &self,
        conn: &mut MultiplexedConnection,
        job_id: &str,
        status: &str,
        retry_count: u8,
    ) -> Result<(), BoxedError> {
        todo!()
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
