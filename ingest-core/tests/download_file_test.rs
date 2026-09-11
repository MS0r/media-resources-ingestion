//! End-to-end test of the `DownloadFile` RPC. Stores a chunked file
//! fixture in MongoDB + the local filesystem, starts a real
//! `IngestServer`, opens a gRPC client, and verifies the reassembled
//! bytes match the original.
//!
//! Requires MongoDB on `MONGODB_URI` (default `root:example@localhost:27017`).
//! The test creates a temp directory under `/tmp/ingest-inttest-download-*`
//! and cleans it up at the end.

use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use flate2::Compression;
use flate2::write::GzEncoder;
use futures_util::StreamExt;
use ingest_core::auth::AuthProviderRegistry;
use ingest_core::models::{ChunkRef, Manifest, Metadata};
use ingest_core::server::proto::ingest_service_client::IngestServiceClient;
use ingest_core::server::proto::ingest_service_server::IngestServiceServer;
use ingest_core::server::{self, proto};
use ingest_core::settings::load_toml;
use ingest_core::storage::ProviderCache;
use mongodb::bson::DateTime as MongoDateTime;
use sha2::{Digest, Sha256};
use std::io::Write;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tonic::transport::{Channel, Endpoint, Server};

const TOML_TEST: &str = r#"
[scheduler]
file_workers = 2
chunk_workers = 4
max_pending_jobs = 100
max_per_host = 2
[compression]
threshold_mb = 512
quality = 95
[storage]
default_provider = "local"
default_path = "/tmp/ingest-inttest"
chunk_size = "128MB"
temp_dir = "/tmp/ingest-inttest"
"#;

fn mongo_uri() -> String {
    std::env::var("MONGODB_URI").unwrap_or_else(|_| {
        "mongodb://root:example@localhost:27017/ingestion?authSource=admin".into()
    })
}

fn sha256_hex(data: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(data);
    hex::encode(h.finalize())
}

async fn gzip_to_path(path: &PathBuf, bytes: &[u8]) {
    let mut f = std::fs::File::create(path).unwrap();
    let mut enc = GzEncoder::new(&mut f, Compression::new(6));
    enc.write_all(bytes).unwrap();
    enc.finish().unwrap();
}

async fn start_test_server() -> (
    IngestServiceClient<Channel>,
    oneshot::Sender<()>,
    TempDir,
    String,
) {
    let tmp = TempDir::new().expect("temp dir");
    let toml_path = tmp.path().join("ingest.toml");
    std::fs::write(&toml_path, TOML_TEST).unwrap();

    let toml_config = load_toml(&toml_path).expect("toml");
    let mongo = ingest_core::MongoService::new(
        &mongo_uri(),
        toml_config.scheduler.mongo_pool_min,
        toml_config.scheduler.mongo_pool_max,
    )
    .await
    .expect("mongo connect");
    let redis = ingest_core::services::redis::RedisService::new(
        &std::env::var("REDIS_URI").unwrap_or_else(|_| "redis://localhost:6379".into()),
        3600,
        3,
        vec![5, 30, 120],
    )
    .expect("redis");
    let provider_cache = Arc::new(ProviderCache::new(AuthProviderRegistry::new()));

    let ingest_server = server::IngestServer::new_from_parts(
        mongo,
        redis,
        toml_config,
        "redis://localhost:6379".into(),
        mongo_uri(),
        provider_cache,
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let (tx, rx) = oneshot::channel::<()>();

    let _server_task = tokio::spawn(async move {
        Server::builder()
            .add_service(IngestServiceServer::new(ingest_server))
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = rx.await;
            })
            .await
            .expect("server failed");
    });

    let endpoint = Endpoint::from_shared(format!("http://{addr}"))
        .expect("endpoint")
        .connect_lazy();
    let client = IngestServiceClient::new(endpoint);

    // Give the server a tick to start accepting.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    (client, tx, tmp, toml_path.to_string_lossy().to_string())
}

#[tokio::test]
async fn download_file_non_chunked_roundtrip() {
    let (mut client, _shutdown, _tmp, _toml) = start_test_server().await;

    // Distinct payload from the chunked test to avoid races on the
    // shared `files_metadata` document when tests run concurrently.
    let payload: Vec<u8> = (0..4096u32)
        .map(|i| ((i + 7).wrapping_mul(13) % 251) as u8)
        .collect();
    let file_hash = sha256_hex(&payload);
    let stored_path = format!("/tmp/ingest-inttest-dl/{file_hash}.bin");
    std::fs::create_dir_all("/tmp/ingest-inttest-dl").unwrap();
    std::fs::write(&stored_path, &payload).unwrap();

    let metadata = Metadata {
        file_hash: file_hash.clone(),
        original_url: url::Url::parse("https://example.com/file.bin").unwrap(),
        storage_provider: ingest_core::storage::Provider::Local,
        storage_path: stored_path.clone(),
        original_file_size: payload.len() as u64,
        compressed_file_size: None,
        compression_ratio: None,
        mime_type: "application/octet-stream".into(),
        chunk_manifest: None,
        upload_date: MongoDateTime::now(),
        duplicate_reference_count: 1,
        update_date: None,
    };

    // Insert directly into Mongo so the server can find it.
    let mongo = ingest_core::MongoService::new(&mongo_uri(), 1, 4)
        .await
        .expect("mongo");
    mongo.insert_test_metadata(&metadata).await.expect("insert");

    // Call the RPC.
    let response = client
        .download_file(proto::DownloadFileRequest {
            hash: file_hash.clone(),
            range_start: None,
            range_end: None,
        })
        .await
        .expect("rpc")
        .into_inner();

    let mut stream = response;
    let mut collected = Vec::new();
    let mut got_header = false;
    while let Some(msg) = stream.next().await {
        let chunk = msg.expect("stream");
        match chunk.payload {
            Some(proto::download_file_chunk::Payload::Header(h)) => {
                assert_eq!(h.file_hash, file_hash);
                assert_eq!(h.total_size, payload.len() as u64);
                assert!(!h.is_chunked);
                got_header = true;
            }
            Some(proto::download_file_chunk::Payload::Data(b)) => {
                collected.extend_from_slice(&b);
            }
            None => {}
        }
    }
    assert!(got_header);
    assert_eq!(collected, payload);

    // Clean up the metadata.
    let _ = mongo.delete_test_metadata(&file_hash).await;
    std::fs::remove_file(&stored_path).ok();
}

#[tokio::test]
async fn download_file_chunked_gzip_roundtrip() {
    let (mut client, _shutdown, _tmp, _toml) = start_test_server().await;

    let payload: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
    let file_hash = sha256_hex(&payload);
    let chunk_a = &payload[..2048];
    let chunk_b = &payload[2048..];

    let dir = PathBuf::from(format!("/tmp/ingest-inttest-dl/{file_hash}"));
    std::fs::create_dir_all(&dir).unwrap();
    let chunk_a_path = dir.join("chunk_00000.bin.gz");
    let chunk_b_path = dir.join("chunk_00001.bin.gz");
    gzip_to_path(&chunk_a_path, chunk_a).await;
    gzip_to_path(&chunk_b_path, chunk_b).await;

    let manifest = Manifest {
        chunks: vec![
            ChunkRef {
                hash: sha256_hex(chunk_a),
                size_original: 2048,
                size_compressed: None,
                storage_path: chunk_a_path.to_string_lossy().to_string(),
                offset_start: 0,
                offset_end: 2047,
            },
            ChunkRef {
                hash: sha256_hex(chunk_b),
                size_original: 2048,
                size_compressed: None,
                storage_path: chunk_b_path.to_string_lossy().to_string(),
                offset_start: 2048,
                offset_end: 4095,
            },
        ],
        compression: Some("gzip".into()),
        original_size: 4096,
        compressed_size: 4096,
    };

    let metadata = Metadata {
        file_hash: file_hash.clone(),
        original_url: url::Url::parse("https://example.com/big.bin").unwrap(),
        storage_provider: ingest_core::storage::Provider::Local,
        storage_path: dir.to_string_lossy().to_string(),
        original_file_size: 4096,
        compressed_file_size: Some(4096),
        compression_ratio: None,
        mime_type: "application/octet-stream".into(),
        chunk_manifest: Some(manifest),
        upload_date: MongoDateTime::now(),
        duplicate_reference_count: 1,
        update_date: None,
    };

    let mongo = ingest_core::MongoService::new(&mongo_uri(), 1, 4)
        .await
        .expect("mongo");
    mongo.insert_test_metadata(&metadata).await.expect("insert");

    let response = client
        .download_file(proto::DownloadFileRequest {
            hash: file_hash.clone(),
            range_start: None,
            range_end: None,
        })
        .await
        .expect("rpc")
        .into_inner();

    let mut stream = response;
    let mut collected = Vec::new();
    let mut got_header = false;
    while let Some(msg) = stream.next().await {
        let chunk = msg.expect("stream");
        match chunk.payload {
            Some(proto::download_file_chunk::Payload::Header(h)) => {
                assert!(h.is_chunked);
                got_header = true;
            }
            Some(proto::download_file_chunk::Payload::Data(b)) => {
                collected.extend_from_slice(&b);
            }
            None => {}
        }
    }
    assert!(got_header);
    assert_eq!(collected, payload);

    // Verify chunk-by-chunk using range requests.
    let response = client
        .download_file(proto::DownloadFileRequest {
            hash: file_hash.clone(),
            range_start: Some(1000),
            range_end: Some(1999),
        })
        .await
        .expect("rpc")
        .into_inner();
    let mut stream = response;
    let mut collected = Vec::new();
    while let Some(msg) = stream.next().await {
        let chunk = msg.expect("stream");
        if let Some(proto::download_file_chunk::Payload::Data(b)) = chunk.payload {
            collected.extend_from_slice(&b);
        }
    }
    assert_eq!(collected, &payload[1000..2000]);

    // Clean up.
    let _ = mongo.delete_test_metadata(&file_hash).await;
    std::fs::remove_dir_all(&dir).ok();
    std::fs::remove_file("/tmp/ingest-inttest-dl").ok();
}

// Suppress unused-import warning for Bytes (used in compile-time test scaffolding).
#[allow(dead_code)]
fn _ref(_b: Bytes) {}
