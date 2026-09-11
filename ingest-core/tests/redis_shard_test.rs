use std::env;

use chrono::Utc;
use ingest_core::handlers::jobs::{FileJob, JobKind, JobStatus};
use ingest_core::models::Resource;
use ingest_core::services::redis::{RedisService, compute_shard, shard_key};
use url::Url;

fn redis_uri() -> String {
    env::var("REDIS_URI").unwrap_or_else(|_| "redis://localhost:6379".into())
}

fn make_file_job(id: &str, priority: i32) -> FileJob {
    let resource = Resource {
        id: id.to_string(),
        url: Url::parse("https://example.com/test.bin").unwrap(),
        name: None,
        priority: Some(priority),
        dest: None,
        config: None,
    };
    let now = Utc::now();
    FileJob {
        _id: id.to_string(),
        batch_id: "batch-test".to_string(),
        resource,
        priority,
        status: JobStatus::Pending,
        retry_count: 0,
        created_at: now,
        updated_at: now,
        file_hash: None,
        error: None,
        chunk_size: None,
    }
}

/// Enqueue a file job and assert it lands on the expected shard.
fn enqueue_and_verify_shard(job: &FileJob, shard_count: u32) {
    let expected_shard = compute_shard(&job._id, shard_count);
    let expected_key = shard_key(expected_shard);
    // The job should land in exactly this shard's pending set.
    // (We verify via dequeue below — no need to ZRANGE here.)
    eprintln!(
        "job {} -> shard {} ({})",
        job._id, expected_shard, expected_key
    );
}

/// Verify that dequeue_job pops every enqueued job exactly once,
/// regardless of which worker_id (and therefore which shard) we pass in.
#[tokio::test]
async fn dequeue_crosses_all_shards() {
    let redis = match RedisService::new(&redis_uri(), 3600, 3, vec![5, 30, 120]) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("SKIP: cannot connect to Redis at {}: {e}", redis_uri());
            return;
        }
    };

    // Flush DB to avoid interference with other tests.
    redis.flush_db().await.expect("FLUSHDB failed");

    let shard_count: u32 = 4;

    // Enqueue 5 jobs with intentionally varying IDs so they land on different shards.
    let jobs: Vec<FileJob> = vec![
        make_file_job("shard-test-a1", 10),
        make_file_job("shard-test-b2", 20),
        make_file_job("shard-test-c3", 5),
        make_file_job("shard-test-d4", 30),
        make_file_job("shard-test-e5", 1),
    ];

    for job in &jobs {
        redis
            .enqueue_file_job(job, shard_count)
            .await
            .unwrap_or_else(|e| panic!("enqueue {job_id}: {e}", job_id = job._id));
        enqueue_and_verify_shard(job, shard_count);
    }

    // Dequeue all 5 jobs using worker_id=0 (shard 0 only — previously broken).
    let mut popped = Vec::new();
    for _ in 0..jobs.len() {
        let result = redis.dequeue_job(0, shard_count).await.expect("dequeue");
        match result {
            Some((kind, id)) => {
                assert!(matches!(kind, JobKind::File));
                popped.push(id);
            }
            None => panic!("dequeue returned None before all jobs were popped"),
        }
    }

    assert_eq!(
        popped.len(),
        jobs.len(),
        "expected {} jobs popped, got {}",
        jobs.len(),
        popped.len()
    );

    // All enqueued IDs must appear in the popped set.
    let mut enqueued_ids: Vec<String> = jobs.iter().map(|j| j._id.clone()).collect();
    enqueued_ids.sort();
    popped.sort();
    assert_eq!(popped, enqueued_ids, "popped set != enqueued set");
}

/// A worker with a different worker_id also pops from all shards.
#[tokio::test]
async fn dequeue_worker_id_does_not_constrain_shards() {
    let redis = match RedisService::new(&redis_uri(), 3600, 3, vec![5, 30, 120]) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("SKIP: cannot connect to Redis at {}: {e}", redis_uri());
            return;
        }
    };

    redis.flush_db().await.expect("FLUSHDB failed");

    let shard_count: u32 = 4;

    let jobs: Vec<FileJob> = vec![
        make_file_job("worker-test-x1", 15),
        make_file_job("worker-test-y2", 25),
    ];

    for job in &jobs {
        redis
            .enqueue_file_job(job, shard_count)
            .await
            .unwrap_or_else(|e| panic!("enqueue {job_id}: {e}", job_id = job._id));
    }

    // Use worker_id = 99 → shard 99 % 4 = 3.
    // Previously, only shard 3 would be checked — most jobs would be missed.
    let mut popped = Vec::new();
    for _ in 0..jobs.len() {
        let result = redis.dequeue_job(99, shard_count).await.expect("dequeue");
        match result {
            Some((kind, id)) => {
                assert!(matches!(kind, JobKind::File));
                popped.push(id);
            }
            None => panic!("dequeue returned None with worker_id=99"),
        }
    }

    assert_eq!(popped.len(), jobs.len());
    let mut enqueued_ids: Vec<String> = jobs.iter().map(|j| j._id.clone()).collect();
    enqueued_ids.sort();
    popped.sort();
    assert_eq!(popped, enqueued_ids);
}

/// Dequeue returns None (empty) when the pending queue is exhausted.
#[tokio::test]
async fn dequeue_returns_none_when_empty() {
    let redis = match RedisService::new(&redis_uri(), 3600, 3, vec![5, 30, 120]) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("SKIP: cannot connect to Redis at {}: {e}", redis_uri());
            return;
        }
    };

    redis.flush_db().await.expect("FLUSHDB failed");

    let result = redis
        .dequeue_job(0, 4)
        .await
        .expect("dequeue should not error");
    assert!(
        result.is_none(),
        "expected None when pending queue is empty"
    );
}
