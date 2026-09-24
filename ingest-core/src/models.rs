pub use crate::compression::{
    CompressionOverride, GenericCompressionStrategy, ImageCompressionStrategy,
    UniversalCompressionStrategy, VideoCompressionStrategy,
};
pub use crate::config::app::{AppConfig, OutputFormat, extract_run_config, load_env_uris};
pub use crate::config::ingest_yaml::{
    Destination, IngestionConfig, Resource, ResourceLevelConfig, load_config,
};
pub use crate::config::toml::TomlRawConfig;
pub use crate::domain::Headers;
pub use crate::storage::Provider;
use mongodb::bson::DateTime as MongoDateTime;
use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    pub file_hash: String,
    pub original_url: Url,
    pub storage_provider: Provider,
    pub storage_path: String,
    pub original_file_size: u64,
    pub compressed_file_size: Option<u64>,
    pub compression_ratio: Option<f32>,
    pub mime_type: String,
    pub chunk_manifest: Option<Manifest>,
    pub upload_date: MongoDateTime,
    #[serde(default)]
    pub duplicate_reference_count: u32,
    pub update_date: Option<MongoDateTime>,
}

impl Metadata {
    pub fn new(
        file_hash: String,
        original_url: Url,
        storage_provider: Provider,
        storage_path: String,
        original_file_size: u64,
        compressed_file_size: Option<u64>,
        mime_type: String,
    ) -> Self {
        Self {
            file_hash,
            original_url,
            storage_provider,
            storage_path,
            original_file_size,
            compressed_file_size,
            compression_ratio: compressed_file_size.map(|c| {
                if original_file_size > 0 {
                    c as f32 / original_file_size as f32
                } else {
                    1.0
                }
            }),
            mime_type,
            chunk_manifest: None,
            upload_date: MongoDateTime::now(),
            duplicate_reference_count: 1,
            update_date: None,
        }
    }

    pub fn with_manifest(mut self, manifest: Manifest) -> Self {
        self.chunk_manifest = Some(manifest);
        self
    }
}

impl std::fmt::Display for Metadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "file_hash={}, original_url={}, storage_provider={}, storage_path={}, size={}, mime={}",
            self.file_hash,
            self.original_url,
            self.storage_provider,
            self.storage_path,
            self.original_file_size,
            self.mime_type,
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub chunks: Vec<ChunkRef>,
    pub compression: Option<String>,
    pub original_size: u64,
    pub compressed_size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkRef {
    pub hash: String,
    pub size_original: u64,
    pub size_compressed: Option<u64>,
    pub storage_path: String,
    /// Per-chunk compression strategy name (e.g. `"gzip"`, `"zstd"`, `""`).
    /// Reflects the *actually applied* strategy, not the requested one. The
    /// reassembler reads this field to decide whether to decompress each
    /// chunk. Empty string means no chunk-level compression (passthrough).
    pub compression: String,
    pub offset_start: u64,
    pub offset_end: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProgressJobType {
    FileJob,
    ChunkJob,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProgressStatus {
    Running,
    Done,
    Failed,
    Retrying,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressEvent {
    pub job_id: String,
    pub job_type: ProgressJobType,
    pub stage: String,
    pub current: u32,
    pub total: Option<u32>,
    pub status: ProgressStatus,
    pub message: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Provider;
    use url::Url;

    #[test]
    fn test_metadata_new_defaults() {
        let url = Url::parse("https://example.com/file.png").unwrap();
        let meta = Metadata::new(
            "abc123".into(),
            url.clone(),
            Provider::Local,
            "/tmp/file.png".into(),
            1024,
            None,
            "image/png".into(),
        );
        assert_eq!(meta.file_hash, "abc123");
        assert_eq!(meta.original_url, url);
        assert_eq!(meta.storage_provider, Provider::Local);
        assert_eq!(meta.storage_path, "/tmp/file.png");
        assert_eq!(meta.original_file_size, 1024);
        assert!(meta.compressed_file_size.is_none());
        assert!(meta.compression_ratio.is_none());
    }

    #[test]
    fn test_metadata_with_compression() {
        let url = Url::parse("https://example.com/file.png").unwrap();
        let meta = Metadata::new(
            "abc123".into(),
            url,
            Provider::Local,
            "/tmp/file.png".into(),
            1000,
            Some(500),
            "image/webp".into(),
        );
        assert_eq!(meta.compressed_file_size, Some(500));
        let ratio = meta.compression_ratio.unwrap();
        assert!((ratio - 0.5).abs() < f32::EPSILON);
    }

    #[test]
    fn test_metadata_with_manifest() {
        let url = Url::parse("https://example.com/file.bin").unwrap();
        let manifest = Manifest {
            chunks: vec![ChunkRef {
                hash: "chunk1hash".into(),
                size_original: 500,
                size_compressed: Some(450),
                storage_path: "/tmp/chunk1.bin".into(),
                compression: "".to_string(),
                offset_start: 0,
                offset_end: 499,
            }],
            compression: None,
            original_size: 1000,
            compressed_size: 900,
        };
        let meta = Metadata::new(
            "abc123".into(),
            url,
            Provider::Local,
            "/tmp/".into(),
            1000,
            Some(900),
            "application/octet-stream".into(),
        )
        .with_manifest(manifest);
        assert!(meta.chunk_manifest.is_some());
        assert_eq!(meta.chunk_manifest.unwrap().chunks.len(), 1);
    }

    #[test]
    fn test_metadata_display() {
        let url = Url::parse("https://example.com/file.png").unwrap();
        let meta = Metadata::new(
            "abc123".into(),
            url,
            Provider::Local,
            "/tmp/file.png".into(),
            1024,
            None,
            "image/png".into(),
        );
        let display = format!("{}", meta);
        assert!(display.contains("file_hash=abc123"));
        assert!(display.contains("size=1024"));
    }
}
