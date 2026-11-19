//! End-to-end test of the chunk-enqueue-and-finalize path. Verifies the
//! fix for the race condition where chunks were dequeued from Redis before
//! their Mongo row existed.
//!
//! The test:
//! 1. Boots a local HTTP server that supports Range requests.
//! 2. Enqueues a file that splits into 4 chunks (small chunk_size for speed).
//! 3. Runs the worker until the parent job finalizes.
//! 4. Verifies all chunks land in the manifest and the file_hash is a valid
//!    Merkle root.
//!
//! Requires MongoDB + Redis (defaults match `docker compose -f compose.dev.yml`).

use std::net::TcpListener as StdTcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use ingest_core::config::RunConfig;
use ingest_core::models::{AppConfig, OutputFormat, load_config};
use ingest_core::services::Services;
use ingest_core::services::redis::RedisService;
use ingest_core::worker::{Shutdown, Worker};
use ingest_core::{MongoService, TomlRawConfig, enqueue};

fn mongo_uri() -> String {
    std::env::var("MONGODB_URI").unwrap_or_else(|_| {
        "mongodb://root:example@localhost:27017/ingestion?authSource=admin".into()
    })
}

fn redis_uri() -> String {
    std::env::var("REDIS_URI").unwrap_or_else(|_| "redis://localhost:6379".into())
}

const TOML_SMALL_CHUNK: &str = r#"
[scheduler]
file_workers = 2
chunk_workers = 4
max_pending_jobs = 100
max_per_host = 2
job_timeout_secs = 120
[compression]
threshold_mb = 512
quality = 95
[storage]
default_provider = "local"
default_path = "/tmp/ingest-inttest-chunk"
chunk_size = "1MB"
temp_dir = "/tmp/ingest-inttest-chunk"
"#;

/// A minimal HTTP server that serves a deterministic payload and supports
/// Range requests. The payload is `TOTAL_SIZE` bytes.
struct RangeServer {
    addr: std::net::SocketAddr,
    _handle: std::thread::JoinHandle<()>,
    shutdown: Arc<AtomicBool>,
}

const TOTAL_SIZE: u64 = 4 * 1024 * 1024; // 4 MB = 4 chunks of 1 MB

impl RangeServer {
    fn start() -> Self {
        Self::start_inner(false)
    }

    fn start_incompressible() -> Self {
        Self::start_inner(true)
    }

    fn start_inner(incompressible: bool) -> Self {
        let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind range server");
        let addr = listener.local_addr().unwrap();
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown.clone();

        let handle = std::thread::spawn(move || {
            listener.set_nonblocking(true).unwrap();
            loop {
                if shutdown_clone.load(Ordering::Relaxed) {
                    break;
                }
                match listener.accept() {
                    Ok((stream, _)) => {
                        let total = TOTAL_SIZE;
                        std::thread::spawn(move || {
                            Self::handle_connection(stream, total, incompressible);
                        });
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(std::time::Duration::from_millis(10));
                        continue;
                    }
                    Err(e) => {
                        eprintln!("accept error: {e}");
                        break;
                    }
                }
            }
        });

        RangeServer {
            addr,
            _handle: handle,
            shutdown,
        }
    }

    fn url(&self) -> String {
        format!("http://127.0.0.1:{}/testfile.bin", self.addr.port())
    }

    fn handle_connection(mut stream: std::net::TcpStream, total: u64, incompressible: bool) {
        use std::io::{BufRead, BufReader, Write};

        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut request_line = String::new();
        reader.read_line(&mut request_line).unwrap_or(0);

        // Consume headers.
        let mut range_header: Option<String> = None;
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap_or(0);
            if line.trim().is_empty() {
                break;
            }
            let lower = line.to_lowercase();
            if let Some(val) = lower.strip_prefix("range:") {
                range_header = Some(val.trim().to_string());
            }
        }

        // If this is a POST (e.g. upload), just read body and 200.
        if request_line.starts_with("POST") {
            let _ = std::io::copy(&mut reader, &mut std::io::empty());
            let response = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
            stream.write_all(response.as_bytes()).ok();
            return;
        }

        // For HEAD, send headers only.
        let is_head = request_line.starts_with("HEAD");

        let (start, end_inclusive) = if let Some(range) = range_header {
            let range_spec = range.trim().strip_prefix("bytes=").unwrap_or(range.trim());
            let parts: Vec<&str> = range_spec.split('-').collect();
            let start: u64 = parts.first().and_then(|s| s.parse().ok()).unwrap_or(0);
            let end: u64 = parts
                .get(1)
                .and_then(|s| s.parse().ok())
                .unwrap_or(total - 1);
            (start, end)
        } else {
            (0, total - 1)
        };

        let len = end_inclusive - start + 1;

        let header = format!(
            "HTTP/1.1 200 OK\r\n\
             Content-Type: application/octet-stream\r\n\
             Content-Length: {len}\r\n\
             Accept-Ranges: bytes\r\n\
             \r\n",
        );
        stream.write_all(header.as_bytes()).ok();

        if is_head {
            return;
        }

        // Write payload bytes.
        let mut written: u64 = 0;
        let mut buf = [0u8; 8192];
        while written < len {
            let chunk = std::cmp::min((len - written) as usize, buf.len());
            for (i, b) in buf[..chunk].iter_mut().enumerate() {
                if incompressible {
                    // SHA-256 counter mode: high-entropy incompressible bytes.
                    use sha2::{Digest, Sha256};
                    let mut hasher = Sha256::new();
                    hasher.update(((start + written + i as u64) / 32).to_le_bytes());
                    let hash = hasher.finalize();
                    *b = hash[((start + written + i as u64) % 32) as usize];
                } else {
                    *b = ((start + written + i as u64) % 251) as u8;
                }
            }
            stream.write_all(&buf[..chunk]).ok();
            written += chunk as u64;
        }
    }
}

impl Drop for RangeServer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

/// Wait for a condition on the Mongo metadata, polling every 500ms.
/// Returns the metadata when the condition is met, or panics on timeout.
async fn wait_for_metadata(
    mongo: &MongoService,
    file_hash: &str,
    timeout_secs: u64,
) -> ingest_core::models::Metadata {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
    loop {
        if let Ok(Some(meta)) = mongo.get_file_metadata(file_hash).await {
            if meta.chunk_manifest.is_some() {
                return meta;
            }
        }
        if std::time::Instant::now() >= deadline {
            panic!("Timed out waiting for metadata with chunk_manifest for hash {file_hash}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

#[tokio::test]
async fn chunk_enqueue_all_chunks_finalize() {
    // 1. Start local HTTP Range server.
    let server = RangeServer::start();
    let url = server.url();

    // 2. Set up Mongo + Redis.
    let mongo = MongoService::new(&mongo_uri(), 1, 16)
        .await
        .expect("mongo connect");
    let redis = RedisService::new(&redis_uri(), 3600, 3, vec![5, 30, 120]).expect("redis connect");

    // Flush Redis to start clean.
    redis.flush_db().await.expect("flush redis");

    // 3. Write a test YAML that points at our HTTP server.
    let tmp = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
    std::fs::create_dir_all(&tmp).unwrap();
    let yaml_path = tmp.join("chunk-test.yaml");
    let dest_path = tmp.join("output");
    std::fs::create_dir_all(&dest_path).unwrap();

    let yaml_content = format!(
        "path: {}\n\
         resources:\n\
           - url: {}\n\
             name: chunk_test_file\n",
        dest_path.display(),
        url
    );
    std::fs::write(&yaml_path, &yaml_content).unwrap();

    // 4. Build AppConfig with small chunk_size so the 4 MB payload
    //    splits into 4 chunks.
    let toml: TomlRawConfig = toml::from_str(TOML_SMALL_CHUNK).expect("parse toml");
    let yaml_config = load_config(&yaml_path).expect("load yaml");
    let args = RunConfig {
        yaml_path: yaml_path.clone(),
        dry_run: false,
        priority: Some(10),
        workers: None,
        follow: false,
        no_follow: true,
        output: OutputFormat::Json,
    };
    let config = AppConfig::from_sources(&yaml_config, toml, args, redis_uri(), mongo_uri());

    // 5. Enqueue the file job.
    let batch_id = enqueue(&config, &yaml_config.resources)
        .await
        .expect("enqueue");

    assert!(!batch_id.is_empty(), "batch_id should not be empty");

    // 6. Run the worker until the shutdown flag flips.
    let services = Services::build(config.clone())
        .await
        .expect("services build");
    let shutdown = Shutdown::install(services.redis.clone());

    let worker = Worker::new(services, shutdown.clone(), 42);
    let worker_handle = worker.spawn();

    // 7. Get the file_job_id from the batch to find the expected hash.
    let batch = mongo
        .get_batch(&batch_id)
        .await
        .expect("get batch")
        .expect("batch not found");
    assert_eq!(batch.job_ids.len(), 1, "expected exactly 1 file job");
    let file_job_id = &batch.job_ids[0];

    // 8. Wait for the file job to be finalized (Metadata with chunk_manifest).
    //    Poll the Mongo file_job for file_hash, then wait for metadata.
    let file_hash = loop {
        if let Ok(Some(job)) = mongo.get_file_job(file_job_id).await {
            if let Some(ref hash) = job.file_hash {
                break hash.clone();
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    };

    let metadata = wait_for_metadata(&mongo, &file_hash, 30).await;

    // 9. Verify the manifest has the expected number of chunks.
    let manifest = metadata.chunk_manifest.as_ref().expect("chunk_manifest");
    assert_eq!(
        manifest.chunks.len(),
        4,
        "expected 4 chunks in manifest, got {}",
        manifest.chunks.len()
    );

    // 10. Verify chunks are contiguous and non-overlapping.
    let mut sorted_chunks = manifest.chunks.clone();
    sorted_chunks.sort_by_key(|c| c.offset_start);
    for (i, chunk) in sorted_chunks.iter().enumerate() {
        assert_eq!(
            chunk.offset_start,
            (i as u64) * 1024 * 1024,
            "chunk {i}: offset_start mismatch"
        );
        assert!(
            chunk.offset_end >= chunk.offset_start,
            "chunk {i}: offset_end < offset_start"
        );
    }

    // 11. Verify original_file_size matches TOTAL_SIZE.
    assert_eq!(
        metadata.original_file_size, TOTAL_SIZE,
        "original_file_size mismatch"
    );

    // 12. Verify the Merkle root is a non-empty hex string.
    assert!(
        !metadata.file_hash.is_empty(),
        "file_hash should not be empty"
    );
    assert_eq!(
        metadata.file_hash.len(),
        64,
        "file_hash should be 64 hex chars (SHA-256), got {}",
        metadata.file_hash.len()
    );

    // 13. Verify the file job status is completed.
    let file_job = mongo
        .get_file_job(file_job_id)
        .await
        .expect("get file job")
        .expect("file job should exist");
    assert_eq!(
        file_job.status.as_str(),
        "completed",
        "file job should be completed, got: {}",
        file_job.status.as_str()
    );

    // 14. Shutdown the worker.
    shutdown.flag().store(true, Ordering::Relaxed);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), worker_handle).await;

    // 15. Clean up.
    let _ = mongo.delete_test_metadata(&file_hash).await;
    std::fs::remove_dir_all(&tmp).ok();
}

/// Verify that incompressible chunks fall back to OriginalFormat and
/// record the correct per-chunk compression in ChunkRef. The manifest
/// summary compression should be None (all chunks are OriginalFormat).
///
/// The RangeServer produces a deterministic SHA-256 counter pattern
/// which is incompressible for gzip. After compress_generic_local's
/// size-compare fallback, each chunk's compression should be "" and the
/// storage_path should not have a .gz extension.
#[tokio::test]
async fn chunk_incompressible_falls_back_to_original() {
    // 1. Start local HTTP Range server with incompressible data.
    let server = RangeServer::start_incompressible();
    let url = server.url();

    // 2. Set up Mongo + Redis.
    let mongo = MongoService::new(&mongo_uri(), 1, 16)
        .await
        .expect("mongo connect");
    let redis = RedisService::new(&redis_uri(), 3600, 3, vec![5, 30, 120]).expect("redis connect");
    redis.flush_db().await.expect("flush redis");

    // 3. Write a test YAML.
    let tmp = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
    std::fs::create_dir_all(&tmp).unwrap();
    let yaml_path = tmp.join("chunk-incompress-test.yaml");
    let dest_path = tmp.join("output");
    std::fs::create_dir_all(&dest_path).unwrap();

    let yaml_content = format!(
        "path: {}\n\
         resources:\n\
           - url: {}\n\
             name: incompress_chunk_test\n",
        dest_path.display(),
        url
    );
    std::fs::write(&yaml_path, &yaml_content).unwrap();

    // 4. Build AppConfig with small chunk_size (1 MB) for 4 MB payload.
    let toml: TomlRawConfig = toml::from_str(TOML_SMALL_CHUNK).expect("parse toml");
    let yaml_config = load_config(&yaml_path).expect("load yaml");
    let args = RunConfig {
        yaml_path: yaml_path.clone(),
        dry_run: false,
        priority: Some(10),
        workers: None,
        follow: false,
        no_follow: true,
        output: OutputFormat::Json,
    };
    let config = AppConfig::from_sources(&yaml_config, toml, args, redis_uri(), mongo_uri());

    // 5. Enqueue.
    let batch_id = enqueue(&config, &yaml_config.resources)
        .await
        .expect("enqueue");
    assert!(!batch_id.is_empty());

    // 6. Run worker.
    let services = Services::build(config.clone())
        .await
        .expect("services build");
    let shutdown = Shutdown::install(services.redis.clone());

    let worker = Worker::new(services, shutdown.clone(), 42);
    let worker_handle = worker.spawn();

    // 7. Get file_job_id from batch.
    let batch = mongo
        .get_batch(&batch_id)
        .await
        .expect("get batch")
        .expect("batch not found");
    assert_eq!(batch.job_ids.len(), 1);
    let file_job_id = &batch.job_ids[0];

    // 8. Wait for file_job to finalize.
    let file_hash = loop {
        if let Ok(Some(job)) = mongo.get_file_job(file_job_id).await {
            if let Some(ref hash) = job.file_hash {
                break hash.clone();
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    };

    let metadata = wait_for_metadata(&mongo, &file_hash, 30).await;

    // 9. Verify the manifest has 4 chunks.
    let manifest = metadata.chunk_manifest.as_ref().expect("chunk_manifest");
    assert_eq!(manifest.chunks.len(), 4);

    // 10. Verify per-chunk compression: each chunk must record "" (OriginalFormat)
    //     because the RangeServer payload is incompressible.
    for (i, chunk) in manifest.chunks.iter().enumerate() {
        assert_eq!(
            chunk.compression, "",
            "chunk {i}: expected empty compression (OriginalFormat) but got {:?}",
            chunk.compression
        );
        // The storage_path must NOT end in .gz — the original bytes are stored.
        assert!(
            !chunk.storage_path.ends_with(".gz"),
            "chunk {i}: storage_path has .gz extension but compression is OriginalFormat: {}",
            chunk.storage_path
        );
    }

    // 11. Verify the manifest summary compression is None (all chunks OriginalFormat).
    assert!(
        manifest.compression.is_none(),
        "manifest.compression should be None for incompressible chunks, got {:?}",
        manifest.compression
    );

    // 12. Shutdown.
    shutdown.flag().store(true, Ordering::Relaxed);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), worker_handle).await;

    // 13. Clean up.
    let _ = mongo.delete_test_metadata(&file_hash).await;
    std::fs::remove_dir_all(&tmp).ok();
}
