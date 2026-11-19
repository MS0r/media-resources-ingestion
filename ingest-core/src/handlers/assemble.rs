//! Reassembly pipeline for stored files.
//!
//! Given a `Metadata` document, walks the manifest (if any), pulls each
//! chunk's bytes from the configured `StorageProvider`, decompresses
//! them on-the-fly, and yields a stream of reassembled bytes back to
//! the caller.
//!
//! For non-chunked files the `storage_path` is downloaded directly and
//! passed through unchanged (a single "chunk" in the abstract sense).
//!
//! Concurrency: chunks are read **sequentially** in `chunk_index` order.
//! Storage backends are typically rate-limited; ordering is required for
//! the reassembled file to be valid.

use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::stream::{self, Stream, StreamExt};
use tokio::io::AsyncReadExt;

use crate::error::JobError;
use crate::handlers::jobs::decompress_generic_reader;
use crate::models::{Manifest, Metadata};
use crate::storage::ProviderCache;

/// One frame of the reassembled stream.
///
/// The first frame is always `Header` — it carries enough metadata for
/// the caller to set `Content-Type`, `Content-Disposition`, etc.
/// Subsequent frames are `Data` (chunked raw bytes).
#[derive(Debug, Clone)]
pub enum Frame {
    Header(HeaderInfo),
    Data(Bytes),
}

#[derive(Debug, Clone)]
pub struct HeaderInfo {
    pub file_hash: String,
    pub mime_type: String,
    /// Filename derived from the storage path's last segment.
    pub filename: String,
    pub total_size: u64,
    pub is_chunked: bool,
}

impl HeaderInfo {
    fn from_metadata(metadata: &Metadata) -> Self {
        let filename = std::path::Path::new(&metadata.storage_path)
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("download")
            .to_string();
        Self {
            file_hash: metadata.file_hash.clone(),
            mime_type: metadata.mime_type.clone(),
            filename,
            total_size: metadata.original_file_size,
            is_chunked: metadata.chunk_manifest.is_some(),
        }
    }
}

/// Reassemble a stored file into a stream of `Frame`s.
///
/// `range_start` / `range_end` (both inclusive, `None` = no limit)
/// forward the `Range` semantics of the public `DownloadFile` RPC. The
/// stream yields the first `Header` followed by enough `Data` frames
/// to cover `[range_start, range_end]` of the original file.
///
/// The return type is `Pin<Box<dyn Stream>>` so callers can poll it
/// via `StreamExt::next` without having to reason about the concrete
/// `Unpin`-ness of the chained stream.
pub fn assemble_file(
    metadata: Metadata,
    provider_cache: Arc<ProviderCache>,
    range_start: Option<u64>,
    range_end: Option<u64>,
) -> Pin<Box<dyn Stream<Item = Result<Frame, JobError>> + Send>> {
    let header = HeaderInfo::from_metadata(&metadata);
    let is_chunked = metadata.chunk_manifest.is_some();

    // First emitted frame is always the header.
    let header_stream = stream::once(async move { Ok(Frame::Header(header)) });

    // The data stream reads bytes in order from the appropriate
    // source. For non-chunked files, it's a single source. For
    // chunked files, we chain each chunk's decompressed stream.
    let provider = metadata.storage_provider.clone();
    let data_stream: Pin<Box<dyn Stream<Item = Result<Frame, JobError>> + Send>> = if is_chunked {
        let manifest = metadata.chunk_manifest.clone().expect("checked is_chunked");
        // Log a warning if any chunk's per-chunk compression differs
        // from the manifest summary. The per-chunk ChunkRef.compression
        // is what actually drives decompression in stream_chunks.
        if let Some(ref mc) = manifest.compression {
            for chunk in &manifest.chunks {
                if !chunk.compression.is_empty() && chunk.compression != mc.as_str() {
                    tracing::warn!(
                        file_hash = %metadata.file_hash,
                        offset_start = chunk.offset_start,
                        chunk_compression = %chunk.compression,
                        manifest_compression = %mc,
                        "Chunk compression differs from manifest summary — \
                         using per-chunk ChunkRef.compression for this chunk"
                    );
                }
            }
        }
        Box::pin(stream_chunks(
            provider,
            provider_cache,
            manifest,
            range_start,
            range_end,
        ))
    } else {
        Box::pin(stream_single(
            provider,
            provider_cache,
            metadata.storage_path.clone(),
            range_start,
            range_end,
        ))
    };

    Box::pin(header_stream.chain(data_stream))
}

/// Single non-chunked file: download from storage, slice the range,
/// emit the bytes.
fn stream_single(
    provider: crate::storage::Provider,
    provider_cache: Arc<ProviderCache>,
    storage_path: String,
    range_start: Option<u64>,
    range_end: Option<u64>,
) -> impl Stream<Item = Result<Frame, JobError>> + Send {
    async_stream::try_stream! {
        let storage = provider_cache.get(&provider);
        let mut input = storage
            .download(&storage_path)
            .await
            .map_err(|e| JobError::OtherFatal(format!("storage download failed: {e}")))?;

        let mut buf = vec![0u8; 64 * 1024];
        let mut absolute_pos: u64 = 0;
        loop {
            let n = input
                .read(&mut buf)
                .await
                .map_err(|e| JobError::OtherFatal(format!("read failed: {e}")))?;
            if n == 0 {
                break;
            }
            let chunk_start = absolute_pos;
            let chunk_end = absolute_pos + n as u64 - 1;
            absolute_pos += n as u64;

            // Range filter: skip bytes outside the requested range.
            let Some((local_start, local_end)) = clip_range(
                chunk_start,
                chunk_end,
                range_start,
                range_end,
            ) else {
                continue;
            };
            let start_in_chunk = (local_start - chunk_start) as usize;
            let end_in_chunk = (local_end - chunk_start + 1) as usize;
            let slice = Bytes::copy_from_slice(&buf[start_in_chunk..end_in_chunk]);
            yield Frame::Data(slice);
        }
    }
}

/// Chunked file: chain each chunk's decompressed stream in order.
fn stream_chunks(
    provider: crate::storage::Provider,
    provider_cache: Arc<ProviderCache>,
    manifest: Manifest,
    range_start: Option<u64>,
    range_end: Option<u64>,
) -> impl Stream<Item = Result<Frame, JobError>> + Send {
    async_stream::try_stream! {
        let mut chunks = manifest.chunks.clone();
        chunks.sort_by_key(|c| c.offset_start);

        let storage = provider_cache.get(&provider);
        for chunk in chunks {
            // Skip chunks entirely outside the range.
            if let Some(rs) = range_start
                && chunk.offset_end < rs
            {
                continue;
            }
            if let Some(re) = range_end
                && chunk.offset_start > re
            {
                break;
            }

            // The local byte range to emit for this chunk.
            let local_start = range_start.unwrap_or(chunk.offset_start).max(chunk.offset_start);
            let local_end = range_end.unwrap_or(chunk.offset_end).min(chunk.offset_end);
            let skip = local_start - chunk.offset_start;
            let take = local_end - local_start + 1;

            let mut raw = storage
                .download(&chunk.storage_path)
                .await
                .map_err(|e| JobError::OtherFatal(format!("chunk download failed: {e}")))?;

            // Pull the raw compressed bytes into a buffer so the
            // decompressor can run. For seek-required formats (zip, 7z)
            // the decompressor buffers internally; for streaming formats
            // (gzip, zstd) we still need a contiguous buffer because the
            // decompressor API takes ownership of the `Read` it wraps.
            let mut buf = Vec::with_capacity((chunk.size_compressed.unwrap_or(chunk.size_original)) as usize);
            raw.read_to_end(&mut buf)
                .await
                .map_err(|e| JobError::OtherFatal(format!("chunk read failed: {e}")))?;

            // Per-chunk compression: use chunk.compression (the
            // actually-applied strategy) rather than the manifest summary.
            let chunk_compression = &chunk.compression;

            let input: Box<dyn tokio::io::AsyncRead + Unpin + Send> =
                Box::new(std::io::Cursor::new(buf));
            // The decoder needs to produce `skip + take` bytes total so
            // we can discard the first `skip` and emit only `take`. Pass
            // `0` for no limit when both are zero.
            let decode_total = skip + take;
            let mut decoder = decompress_generic_reader(
                input,
                chunk_compression,
                if decode_total == 0 { 0 } else { decode_total },
            );

            // Skip the first `skip` decoded bytes (the part of the
            // chunk that lies before the range start).
            if skip > 0 {
                let mut skip_buf = vec![0u8; 64 * 1024];
                let mut remaining = skip;
                while remaining > 0 {
                    let to_read = (remaining as usize).min(skip_buf.len());
                    let n = decoder
                        .read(&mut skip_buf[..to_read])
                        .await
                        .map_err(|e| JobError::OtherFatal(format!("decoder read failed: {e}")))?;
                    if n == 0 {
                        break;
                    }
                    remaining -= n as u64;
                }
            }

            // Stream up to `take` decoded bytes, chunking at 64 KiB.
            let mut out = vec![0u8; 64 * 1024];
            let mut remaining = take;
            while remaining > 0 {
                let want = (remaining as usize).min(out.len());
                let n = decoder
                    .read(&mut out[..want])
                    .await
                    .map_err(|e| JobError::OtherFatal(format!("decoder read failed: {e}")))?;
                if n == 0 {
                    break;
                }
                yield Frame::Data(Bytes::copy_from_slice(&out[..n]));
                remaining -= n as u64;
            }
        }
    }
}

/// Clip an absolute byte range `[chunk_start, chunk_end]` to a
/// requested `[range_start, range_end]`. Returns `None` if the chunk
/// is entirely outside the range, otherwise the inclusive local range.
fn clip_range(
    chunk_start: u64,
    chunk_end: u64,
    range_start: Option<u64>,
    range_end: Option<u64>,
) -> Option<(u64, u64)> {
    let lo = range_start.unwrap_or(0).max(chunk_start);
    let hi = range_end.unwrap_or(u64::MAX).min(chunk_end);
    if lo > hi {
        return None;
    }
    Some((lo, hi))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{ChunkRef, Manifest};
    use crate::storage::Provider;

    fn header_for(metadata: &Metadata) -> HeaderInfo {
        HeaderInfo::from_metadata(metadata)
    }

    #[test]
    fn test_header_info_from_non_chunked() {
        let metadata = Metadata {
            file_hash: "abc".into(),
            original_url: url::Url::parse("https://example.com/file.bin").unwrap(),
            storage_provider: Provider::Local,
            storage_path: "/var/lib/ingest/perro/chunk_00000.bin.gz".into(),
            original_file_size: 1000,
            compressed_file_size: Some(500),
            compression_ratio: Some(0.5),
            mime_type: "image/png".into(),
            chunk_manifest: None,
            upload_date: mongodb::bson::DateTime::now(),
            duplicate_reference_count: 1,
            update_date: None,
        };
        let h = header_for(&metadata);
        assert_eq!(h.filename, "chunk_00000.bin.gz");
        assert_eq!(h.mime_type, "image/png");
        assert_eq!(h.total_size, 1000);
        assert!(!h.is_chunked);
    }

    #[test]
    fn test_header_info_from_chunked() {
        let manifest = Manifest {
            chunks: vec![],
            compression: Some("gzip".into()),
            original_size: 2000,
            compressed_size: 1000,
        };
        let metadata = Metadata {
            file_hash: "xyz".into(),
            original_url: url::Url::parse("https://example.com/file.bin").unwrap(),
            storage_provider: Provider::Local,
            storage_path: "/var/lib/ingest/perro".into(),
            original_file_size: 2000,
            compressed_file_size: Some(1000),
            compression_ratio: Some(0.5),
            mime_type: "video/mp4".into(),
            chunk_manifest: Some(manifest),
            upload_date: mongodb::bson::DateTime::now(),
            duplicate_reference_count: 1,
            update_date: None,
        };
        let h = header_for(&metadata);
        assert_eq!(h.filename, "perro");
        assert!(h.is_chunked);
    }

    #[test]
    fn test_clip_range_inclusive() {
        // Chunk [0, 99], range [10, 19] -> (10, 19)
        assert_eq!(clip_range(0, 99, Some(10), Some(19)), Some((10, 19)));
        // Chunk entirely inside range
        assert_eq!(clip_range(50, 60, Some(10), Some(100)), Some((50, 60)));
        // Chunk entirely before range
        assert_eq!(clip_range(0, 9, Some(10), Some(20)), None);
        // Chunk entirely after range
        assert_eq!(clip_range(100, 199, Some(10), Some(20)), None);
        // No range
        assert_eq!(clip_range(0, 99, None, None), Some((0, 99)));
        // Open-ended range
        assert_eq!(clip_range(0, 99, Some(50), None), Some((50, 99)));
    }

    use crate::auth::AuthProviderRegistry;
    use sha2::{Digest, Sha256};
    use std::io::Write;
    use tokio::io::AsyncWriteExt;

    #[allow(dead_code)]
    async fn write_test_chunk(path: &std::path::Path, bytes: &[u8]) {
        let mut f = tokio::fs::File::create(path).await.unwrap();
        f.write_all(bytes).await.unwrap();
        f.flush().await.unwrap();
    }

    async fn gzip_to_path(path: &std::path::Path, bytes: &[u8]) {
        use flate2::Compression;
        use flate2::write::GzEncoder;
        let mut f = std::fs::File::create(path).unwrap();
        let mut enc = GzEncoder::new(&mut f, Compression::new(6));
        enc.write_all(bytes).unwrap();
        enc.finish().unwrap();
    }

    fn provider_cache_for_test() -> Arc<ProviderCache> {
        Arc::new(ProviderCache::new(AuthProviderRegistry::new()))
    }

    fn sha256_hex(data: &[u8]) -> String {
        let mut h = Sha256::new();
        h.update(data);
        hex::encode(h.finalize())
    }

    #[tokio::test]
    async fn test_assemble_file_non_chunked_local() {
        let dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&dir).unwrap();
        let payload: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
        let path = dir.join("payload.bin");
        std::fs::write(&path, &payload).unwrap();

        let metadata = Metadata {
            file_hash: sha256_hex(&payload),
            original_url: url::Url::parse("https://example.com/file.bin").unwrap(),
            storage_provider: Provider::Local,
            storage_path: path.to_string_lossy().to_string(),
            original_file_size: payload.len() as u64,
            compressed_file_size: None,
            compression_ratio: None,
            mime_type: "application/octet-stream".into(),
            chunk_manifest: None,
            upload_date: mongodb::bson::DateTime::now(),
            duplicate_reference_count: 1,
            update_date: None,
        };

        let cache = provider_cache_for_test();
        let mut stream = assemble_file(metadata, cache, None, None);
        let mut collected = Vec::new();
        while let Some(frame) = stream.next().await {
            match frame.unwrap() {
                Frame::Header(h) => {
                    assert_eq!(h.total_size, payload.len() as u64);
                    assert!(!h.is_chunked);
                }
                Frame::Data(b) => collected.extend_from_slice(&b),
            }
        }
        assert_eq!(collected, payload);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_assemble_file_chunked_gzip_local() {
        let dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&dir).unwrap();

        // Build a 2-chunk manifest. Each chunk is 1024 bytes; total 2048.
        let payload: Vec<u8> = (0..2048u32).map(|i| (i % 251) as u8).collect();
        let chunk_a = &payload[..1024];
        let chunk_b = &payload[1024..];

        let chunk_a_path = dir.join("chunk_00000.bin.gz");
        let chunk_b_path = dir.join("chunk_00001.bin.gz");
        gzip_to_path(&chunk_a_path, chunk_a).await;
        gzip_to_path(&chunk_b_path, chunk_b).await;

        let chunk_ref_a = ChunkRef {
            hash: sha256_hex(chunk_a),
            size_original: 1024,
            size_compressed: None,
            storage_path: chunk_a_path.to_string_lossy().to_string(),
            compression: "gzip".to_string(),
            offset_start: 0,
            offset_end: 1023,
        };
        let chunk_ref_b = ChunkRef {
            hash: sha256_hex(chunk_b),
            size_original: 1024,
            size_compressed: None,
            storage_path: chunk_b_path.to_string_lossy().to_string(),
            compression: "gzip".to_string(),
            offset_start: 1024,
            offset_end: 2047,
        };

        let manifest = Manifest {
            chunks: vec![chunk_ref_a, chunk_ref_b],
            compression: Some("gzip".into()),
            original_size: 2048,
            compressed_size: 2048,
        };

        let metadata = Metadata {
            file_hash: sha256_hex(&payload),
            original_url: url::Url::parse("https://example.com/big.bin").unwrap(),
            storage_provider: Provider::Local,
            storage_path: dir.to_string_lossy().to_string(),
            original_file_size: 2048,
            compressed_file_size: Some(2048),
            compression_ratio: None,
            mime_type: "application/octet-stream".into(),
            chunk_manifest: Some(manifest),
            upload_date: mongodb::bson::DateTime::now(),
            duplicate_reference_count: 1,
            update_date: None,
        };

        let cache = provider_cache_for_test();
        let mut stream = assemble_file(metadata, cache, None, None);
        let mut collected = Vec::new();
        let mut got_header = false;
        while let Some(frame) = stream.next().await {
            match frame.unwrap() {
                Frame::Header(h) => {
                    assert!(h.is_chunked);
                    got_header = true;
                }
                Frame::Data(b) => collected.extend_from_slice(&b),
            }
        }
        assert!(got_header);
        assert_eq!(collected, payload);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_assemble_file_chunked_with_range() {
        let dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&dir).unwrap();

        let payload: Vec<u8> = (0..2048u32).map(|i| (i % 251) as u8).collect();
        let chunk_a = &payload[..1024];
        let chunk_b = &payload[1024..];

        let chunk_a_path = dir.join("chunk_00000.bin.gz");
        let chunk_b_path = dir.join("chunk_00001.bin.gz");
        gzip_to_path(&chunk_a_path, chunk_a).await;
        gzip_to_path(&chunk_b_path, chunk_b).await;

        let manifest = Manifest {
            chunks: vec![
                ChunkRef {
                    hash: sha256_hex(chunk_a),
                    size_original: 1024,
                    size_compressed: None,
                    storage_path: chunk_a_path.to_string_lossy().to_string(),
                    compression: "gzip".to_string(),
                    offset_start: 0,
                    offset_end: 1023,
                },
                ChunkRef {
                    hash: sha256_hex(chunk_b),
                    size_original: 1024,
                    size_compressed: None,
                    storage_path: chunk_b_path.to_string_lossy().to_string(),
                    compression: "gzip".to_string(),
                    offset_start: 1024,
                    offset_end: 2047,
                },
            ],
            compression: Some("gzip".into()),
            original_size: 2048,
            compressed_size: 2048,
        };

        let metadata = Metadata {
            file_hash: sha256_hex(&payload),
            original_url: url::Url::parse("https://example.com/big.bin").unwrap(),
            storage_provider: Provider::Local,
            storage_path: dir.to_string_lossy().to_string(),
            original_file_size: 2048,
            compressed_file_size: Some(2048),
            compression_ratio: None,
            mime_type: "application/octet-stream".into(),
            chunk_manifest: Some(manifest),
            upload_date: mongodb::bson::DateTime::now(),
            duplicate_reference_count: 1,
            update_date: None,
        };

        // Request the middle 100 bytes: [1000, 1099]
        // chunk_a covers [0, 1023] -> [1000, 1023]
        // chunk_b covers [1024, 2047] -> [1024, 1099]
        let cache = provider_cache_for_test();
        let mut stream = assemble_file(metadata, cache, Some(1000), Some(1099));
        let mut collected = Vec::new();
        while let Some(frame) = stream.next().await {
            if let Frame::Data(b) = frame.unwrap() {
                collected.extend_from_slice(&b);
            }
        }
        assert_eq!(collected, &payload[1000..1100]);

        std::fs::remove_dir_all(&dir).ok();
    }
}
