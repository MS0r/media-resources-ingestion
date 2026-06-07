use std::{fmt::Display, path::PathBuf};

use crate::{
    cli::{EnqueueArgs, OutputFormat, RunArgs},
    settings::TomlRawConfig,
    storage::Provider,
};
use mongodb::bson::{DateTime as MongoDateTime, error::Error};
use serde::{Deserialize, Serialize};
use url::Url;

/// Core data models for the media resources ingestion system
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub struct Headers {
    pub authorization: Option<String>,
    pub cookie: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    pub file_hash: String, // SHA256 or similar
    original_url: Url,
    storage_provider: Provider,
    storage_path: String,
    pub original_file_size: u64,
    compressed_file_size: Option<u64>,
    compression_ratio: Option<f32>,
    pub mime_type: String,
    chunk_manifest: Option<Manifest>, // List of chunk identifiers if applicable
    upload_date: MongoDateTime,
    duplicate_reference_count: u32,
    update_date: Option<MongoDateTime>,
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
            compression_ratio: compressed_file_size.map(|c| c as f32 / original_file_size as f32),
            mime_type,
            chunk_manifest: None,
            upload_date: MongoDateTime::now(),
            duplicate_reference_count: 0,
            update_date: None,
        }
    }
}

impl Display for Metadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "File Hash: {}\nOriginal URL: {}\nStorage Provider: {:?}\nMIME Type: {}\nOriginal Size: {} bytes\nCompressed Size: {} bytes\nUpload Date: {}",
            self.file_hash,
            self.original_url,
            self.storage_provider,
            self.mime_type,
            self.original_file_size,
            self.compressed_file_size
                .map_or("N/A".to_string(), |c| c.to_string()),
            self.upload_date
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    chunks: Vec<ChunkRef>,
    compression: Option<String>, // e.g. "image/webp"
    original_size: u64,
    compressed_size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkRef {
    hash: String,
    size_original: u64,
    size_compressed: Option<u64>,
    storage_path: String,
    offset_start: u64,
    offset_end: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ImageCompressionStrategy {
    #[default]
    Avif,
    Webp,
    LosslessWebp,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum VideoCompressionStrategy {
    #[default]
    H264,
    H265,
    Av1,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum GenericCompressionStrategy {
    #[default]
    OriginalFormat,
    Zstd,
    Zip,
    SevenZ,
    None,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum UniversalCompressionStrategy {
    None,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CompressionOverride {
    Image(ImageCompressionStrategy),
    Video(VideoCompressionStrategy),
    Generic(GenericCompressionStrategy),
    Universal(UniversalCompressionStrategy),
}

impl Default for CompressionOverride {
    fn default() -> Self {
        CompressionOverride::Universal(UniversalCompressionStrategy::None)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResourceLevelConfig {
    #[serde(default)]
    pub compression_override: Option<CompressionOverride>,
    pub quality: Option<u8>,
    #[serde(default)]
    pub headers: Option<Headers>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Destination {
    #[serde(default)]
    pub provider: Option<Provider>,
    #[serde(default)]
    pub path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Resource {
    #[serde(default = "default_uuid")]
    pub id: String,
    pub url: Url,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub priority: Option<i32>,
    pub dest: Option<Destination>,
    #[serde(default)]
    pub config: Option<ResourceLevelConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestionConfig {
    #[serde(flatten)]
    pub default_dest: Option<Destination>,
    #[serde(default)]
    priority: Option<i32>,
    chunk_size: Option<String>,
    #[serde(default)]
    compression_override: Option<CompressionOverride>,
    quality: Option<u8>,
    #[serde(default)]
    headers: Option<Headers>,
    pub resources: Vec<Resource>,
}

fn default_uuid() -> String {
    uuid::Uuid::new_v4().to_string()
}

#[derive(Clone)]
pub struct AppConfig {
    // Environment
    pub redis_uri: String,
    pub mongo_uri: String,

    // Scheduler (TOML, CLI --workers overrides file_workers)
    pub file_workers: usize,
    pub chunk_workers: usize,
    pub max_pending_jobs: usize,
    pub max_per_host: usize,
    pub job_timeout_secs: u64,

    // Compression (TOML)
    pub compression_threshold_mb: u64,
    pub compression_quality: u8,
    pub compression_timeout_secs: u64,

    // Storage (TOML, YAML overrides provider/path/chunk_size)
    pub default_provider: String,
    pub default_path: String,
    pub chunk_size: String,
    pub temp_dir: String,

    // Retry (TOML)
    pub running_job_ttl_secs: u64,
    pub max_retries: u8,
    pub backoff_secs: Vec<u64>,

    // Compression + headers + quality (YAML, merged in from_sources)
    pub compression_override: Option<CompressionOverride>,
    pub headers: Option<Headers>,
    pub quality: Option<u8>,

    // Run behavior (CLI, YAML fallback)
    pub yaml_path: PathBuf,
    pub priority: i32,
    pub dry_run: bool,
    pub follow: bool,
    pub output: OutputFormat,
}

pub fn extract_run_config(
    yaml_config: IngestionConfig,
    toml_config: TomlRawConfig,
    run_args: RunArgs,
    redis_uri: String,
    mongo_uri: String,
) -> Result<(AppConfig, Vec<Resource>), Error> {
    let config = AppConfig::from_sources(&yaml_config, toml_config, run_args, redis_uri, mongo_uri);
    let resources = yaml_config.resources;
    Ok((config, resources))
}

pub fn extract_enqueue_config(
    yaml_config: IngestionConfig,
    toml_config: TomlRawConfig,
    enqueue_args: EnqueueArgs,
    redis_uri: String,
    mongo_uri: String,
) -> Result<(AppConfig, Vec<Resource>), Error> {
    let config = AppConfig::from_enqueue_args(
        &yaml_config,
        toml_config,
        enqueue_args,
        redis_uri,
        mongo_uri,
    );
    let resources = yaml_config.resources;
    Ok((config, resources))
}

impl AppConfig {
    pub fn from_sources(
        yaml: &IngestionConfig,
        toml: TomlRawConfig,
        args: RunArgs,
        redis_uri: String,
        mongo_uri: String,
    ) -> Self {
        let (default_provider, default_path) = match &yaml.default_dest {
            Some(dest) => {
                let pr = match &dest.provider {
                    Some(p) => p.to_string(),
                    None => toml.storage.default_provider,
                };
                let pa = match &dest.path {
                    Some(p) => p.to_string(),
                    None => toml.storage.default_path,
                };
                (pr, pa)
            }
            None => (toml.storage.default_provider, toml.storage.default_path),
        };
        let chunk_size = yaml.chunk_size.clone().unwrap_or(toml.storage.chunk_size);
        let priority = args.priority.or(yaml.priority).unwrap_or(0);
        let quality = yaml.quality.or(Some(toml.compression.quality));
        let file_workers = args.workers.unwrap_or(toml.scheduler.file_workers);
        let follow = args.follow || !args.no_follow;

        Self {
            redis_uri,
            mongo_uri,
            file_workers,
            chunk_workers: toml.scheduler.chunk_workers,
            max_pending_jobs: toml.scheduler.max_pending_jobs,
            max_per_host: toml.scheduler.max_per_host,
            job_timeout_secs: toml.scheduler.job_timeout_secs,
            compression_threshold_mb: toml.compression.threshold_mb,
            compression_quality: toml.compression.quality,
            compression_timeout_secs: toml.compression.max_compression_seconds,
            default_provider,
            default_path,
            chunk_size,
            temp_dir: toml.storage.temp_dir,
            running_job_ttl_secs: toml.retry.running_job_ttl_secs,
            max_retries: toml.retry.max_attempts,
            backoff_secs: toml.retry.backoff_secs.clone(),
            compression_override: yaml.compression_override.clone(),
            headers: yaml.headers.clone(),
            quality,
            yaml_path: args.yaml_path.clone(),
            priority,
            dry_run: args.dry_run,
            follow,
            output: args.output,
        }
    }

    pub fn from_worker_args(
        toml: TomlRawConfig,
        redis_uri: String,
        mongo_uri: String,
        workers: Option<usize>,
    ) -> Self {
        let file_workers = workers.unwrap_or(toml.scheduler.file_workers);
        Self {
            redis_uri,
            mongo_uri,
            file_workers,
            chunk_workers: toml.scheduler.chunk_workers,
            max_pending_jobs: toml.scheduler.max_pending_jobs,
            max_per_host: toml.scheduler.max_per_host,
            job_timeout_secs: toml.scheduler.job_timeout_secs,
            compression_threshold_mb: toml.compression.threshold_mb,
            compression_quality: toml.compression.quality,
            compression_timeout_secs: toml.compression.max_compression_seconds,
            default_provider: toml.storage.default_provider,
            default_path: toml.storage.default_path,
            chunk_size: toml.storage.chunk_size,
            temp_dir: toml.storage.temp_dir,
            running_job_ttl_secs: toml.retry.running_job_ttl_secs,
            max_retries: toml.retry.max_attempts,
            backoff_secs: toml.retry.backoff_secs.clone(),
            compression_override: None,
            headers: None,
            quality: None,
            yaml_path: PathBuf::new(),
            priority: 0,
            dry_run: false,
            follow: false,
            output: OutputFormat::Table,
        }
    }

    pub fn from_enqueue_args(
        yaml: &IngestionConfig,
        toml: TomlRawConfig,
        args: EnqueueArgs,
        redis_uri: String,
        mongo_uri: String,
    ) -> Self {
        let (default_provider, default_path) = match &yaml.default_dest {
            Some(dest) => {
                let pr = match &dest.provider {
                    Some(p) => p.to_string(),
                    None => toml.storage.default_provider,
                };
                let pa = match &dest.path {
                    Some(p) => p.to_string(),
                    None => toml.storage.default_path,
                };
                (pr, pa)
            }
            None => (toml.storage.default_provider, toml.storage.default_path),
        };
        let chunk_size = yaml.chunk_size.clone().unwrap_or(toml.storage.chunk_size);
        let priority = args.priority.or(yaml.priority).unwrap_or(0);
        let quality = yaml.quality.or(Some(toml.compression.quality));
        let file_workers = args.workers.unwrap_or(toml.scheduler.file_workers);

        Self {
            redis_uri,
            mongo_uri,
            file_workers,
            chunk_workers: toml.scheduler.chunk_workers,
            max_pending_jobs: toml.scheduler.max_pending_jobs,
            max_per_host: toml.scheduler.max_per_host,
            job_timeout_secs: toml.scheduler.job_timeout_secs,
            compression_threshold_mb: toml.compression.threshold_mb,
            compression_quality: toml.compression.quality,
            compression_timeout_secs: toml.compression.max_compression_seconds,
            default_provider,
            default_path,
            chunk_size,
            temp_dir: toml.storage.temp_dir,
            running_job_ttl_secs: toml.retry.running_job_ttl_secs,
            max_retries: toml.retry.max_attempts,
            backoff_secs: toml.retry.backoff_secs.clone(),
            compression_override: yaml.compression_override.clone(),
            headers: yaml.headers.clone(),
            quality,
            yaml_path: args.yaml_path.clone(),
            priority,
            dry_run: args.dry_run,
            follow: false,
            output: args.output,
        }
    }
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
        assert_eq!(meta.mime_type, "image/png");
        assert!(meta.chunk_manifest.is_none());
        assert_eq!(meta.duplicate_reference_count, 0);
        assert!(meta.update_date.is_none());
    }

    #[test]
    fn test_metadata_new_with_compression() {
        let url = Url::parse("https://example.com/video.mp4").unwrap();
        let meta = Metadata::new(
            "def456".into(),
            url,
            Provider::S3,
            "bucket/key".into(),
            5000,
            Some(2000),
            "video/mp4".into(),
        );
        assert_eq!(meta.compressed_file_size, Some(2000));
        assert!(meta.compression_ratio.is_some());
        assert!((meta.compression_ratio.unwrap() - 0.4).abs() < f32::EPSILON);
    }

    #[test]
    fn test_metadata_serde_roundtrip() {
        let url = Url::parse("https://example.com/image.webp").unwrap();
        let meta = Metadata::new(
            "hash789".into(),
            url,
            Provider::Gdrive,
            "drive:/path".into(),
            100,
            Some(50),
            "image/webp".into(),
        );
        let json = serde_json::to_string(&meta).unwrap();
        let deser: Metadata = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.file_hash, meta.file_hash);
        assert_eq!(deser.original_url, meta.original_url);
        assert_eq!(deser.mime_type, meta.mime_type);
        assert_eq!(deser.original_file_size, meta.original_file_size);
    }

    #[test]
    fn test_manifest_serde() {
        let manifest = Manifest {
            chunks: vec![ChunkRef {
                hash: "c1".into(),
                size_original: 1000,
                size_compressed: Some(500),
                storage_path: "/chunks/c1".into(),
                offset_start: 0,
                offset_end: 999,
            }],
            compression: Some("image/webp".into()),
            original_size: 1000,
            compressed_size: 500,
        };
        let json = serde_json::to_string(&manifest).unwrap();
        let deser: Manifest = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.chunks.len(), 1);
        assert_eq!(deser.chunks[0].hash, "c1");
    }

    #[test]
    fn test_resource_default_uuid() {
        let resource: Resource = serde_yaml::from_str(
            r#"
            url: "https://example.com/file.txt"
        "#,
        )
        .unwrap();
        assert!(!resource.id.is_empty());
        assert_eq!(resource.url.to_string(), "https://example.com/file.txt");
        assert!(resource.name.is_none());
        assert!(resource.priority.is_none());
        assert!(resource.dest.is_none());
        assert!(resource.config.is_none());
    }

    #[test]
    fn test_resource_with_all_fields() {
        let yaml = r#"
            url: "https://example.com/img.png"
            name: my_image
            priority: 5
            dest:
              provider: s3
              path: /bucket/images/
        "#;
        let resource: Resource = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(resource.name.unwrap(), "my_image");
        assert_eq!(resource.priority.unwrap(), 5);
        let dest = resource.dest.unwrap();
        assert_eq!(dest.provider.unwrap().to_string(), "s3");
        assert_eq!(dest.path.unwrap(), "/bucket/images/");
    }

    #[test]
    fn test_ingestion_config_empty_resources() {
        let config: IngestionConfig = serde_yaml::from_str(
            r#"
            resources: []
        "#,
        )
        .unwrap();
        assert!(config.resources.is_empty());
    }

    #[test]
    fn test_ingestion_config_minimal() {
        let config: IngestionConfig = serde_yaml::from_str(
            r#"
            resources:
              - url: "https://example.com/f.png"
        "#,
        )
        .unwrap();
        assert_eq!(config.resources.len(), 1);
        assert!(config.priority.is_none());
        assert!(config.chunk_size.is_none());
    }

    #[test]
    fn test_ingestion_config_with_all_top_level() {
        let config: IngestionConfig = serde_yaml::from_str(
            r#"
            provider: s3
            path: /bucket
            priority: 10
            chunk_size: 256MB
            compression_override: webp
            resources:
              - url: "https://example.com/f.png"
        "#,
        )
        .unwrap();
        let dest = config.default_dest.unwrap();
        assert_eq!(dest.provider.unwrap().to_string(), "s3");
        assert_eq!(dest.path.unwrap(), "/bucket");
        assert_eq!(config.priority.unwrap(), 10);
        assert_eq!(config.chunk_size.unwrap(), "256MB");
    }

    #[test]
    fn test_destination_default_serde() {
        let dest: Destination = serde_yaml::from_str(
            r#"
            path: /some/path
        "#,
        )
        .unwrap();
        assert!(dest.provider.is_none());
        assert_eq!(dest.path.unwrap(), "/some/path");
    }

    #[test]
    fn test_compression_override_webp() {
        let override_val: CompressionOverride = serde_yaml::from_str("webp").unwrap();
        assert!(matches!(
            override_val,
            CompressionOverride::Image(ImageCompressionStrategy::Webp)
        ));
    }

    #[test]
    fn test_compression_override_none() {
        let override_val: CompressionOverride = serde_yaml::from_str("none").unwrap();
        assert!(matches!(
            override_val,
            CompressionOverride::Generic(GenericCompressionStrategy::None)
        ));
    }

    #[test]
    fn test_resource_level_config_serde() {
        let config: ResourceLevelConfig = serde_yaml::from_str(
            r#"
            quality: 85
            compression_override: avif
        "#,
        )
        .unwrap();
        assert_eq!(config.quality.unwrap(), 85);
        assert!(config.compression_override.is_some());
    }

    #[test]
    fn test_headers_serde() {
        let headers: Headers = serde_yaml::from_str(
            r#"
            authorization: "Bearer token123"
            cookie: "session=abc"
        "#,
        )
        .unwrap();
        assert_eq!(headers.authorization.unwrap(), "Bearer token123");
        assert_eq!(headers.cookie.unwrap(), "session=abc");
    }

    #[test]
    fn test_compression_override_all_image_variants() {
        let cases = [
            ("webp", ImageCompressionStrategy::Webp),
            ("avif", ImageCompressionStrategy::Avif),
            ("losslesswebp", ImageCompressionStrategy::LosslessWebp),
        ];
        for (yaml, expected) in &cases {
            let val: CompressionOverride = serde_yaml::from_str(yaml).unwrap();
            assert!(
                matches!(&val, CompressionOverride::Image(s) if s == expected),
                "failed for input {yaml:?}, got {val:?}"
            );
        }
    }

    #[test]
    fn test_compression_override_all_video_variants() {
        let cases = [
            ("h265", VideoCompressionStrategy::H265),
            ("av1", VideoCompressionStrategy::Av1),
        ];
        for (yaml, expected) in &cases {
            let val: CompressionOverride = serde_yaml::from_str(yaml).unwrap();
            assert!(
                matches!(&val, CompressionOverride::Video(s) if s == expected),
                "failed for input {yaml:?}, got {val:?}"
            );
        }
    }

    #[test]
    fn test_resource_level_config_with_all_overrides() {
        let overrides = [
            "webp",
            "avif",
            "losslesswebp",
            "originalformat",
            "h265",
            "av1",
            "zstd",
            "zip",
            "sevenz",
            "none",
        ];
        for co in &overrides {
            let yaml = format!(
                r#"
                quality: 90
                compression_override: {co}
            "#
            );
            let config: ResourceLevelConfig = serde_yaml::from_str(&yaml).unwrap();
            assert_eq!(config.quality, Some(90));
            assert!(config.compression_override.is_some(), "failed for {co}");
        }
    }

    #[test]
    fn test_chunk_ref_defaults() {
        let cr: ChunkRef = serde_json::from_str(
            r#"
            {"hash": "c1", "size_original": 100, "size_compressed": null, "storage_path": "/p", "offset_start": 0, "offset_end": 99}
        "#,
        )
        .unwrap();
        assert_eq!(cr.hash, "c1");
        assert!(cr.size_compressed.is_none());
    }

    // --- config_test.rs YAML tests ---

    const YAML_FULL: &str = r#"
provider: local
path: ~/images
priority: 0
chunk_size: 128MB
compression_override: webp
headers:
  authorization: Bearer Token
  cookie: session=abc
resources:
  - url: https://example.com/image.webp
    name: image
    priority: 10
    dest:
      provider: local
      path: ~/downloads
    config:
      compression_override: webp
      quality: 95
"#;

    const YAML_MINIMAL: &str = r#"
resources:
  - url: https://example.com/file.txt
"#;

    const YAML_MULTIPLE_RESOURCES: &str = r#"
provider: s3
path: /mnt/bucket
priority: 5
resources:
  - url: https://example.com/image1.png
    name: first_image
    priority: 10
  - url: https://example.com/image2.jpg
    name: second_image
  - url: https://example.com/image3.gif
"#;

    const YAML_DEST: &str = r#"
resources:
  - url: https://example.com/test.png
    dest:
      path: /custom/path
"#;

    const YAML_HEADERS: &str = r#"
headers:
  authorization: Bearer mytoken123
resources:
  - url: https://api.example.com/data.json
"#;

    const YAML_RESOURCE_CONFIG: &str = r#"
resources:
  - url: https://example.com/image.png
    config:
      quality: 80
"#;

    #[test]
    fn test_yaml_config_full_deserialization() {
        let config: IngestionConfig =
            serde_yaml::from_str(YAML_FULL).expect("Failed to parse YAML");

        assert!(config.default_dest.is_some());
        let default_dest = config.default_dest.unwrap();
        assert!(default_dest.provider.is_some());
        assert_eq!(default_dest.provider.unwrap().to_string(), "local");
        assert_eq!(default_dest.path.unwrap(), "~/images");
        assert_eq!(config.priority, Some(0));
        assert_eq!(config.chunk_size, Some("128MB".to_string()));

        let headers = config.headers.expect("headers should be present");
        assert_eq!(headers.authorization, Some("Bearer Token".to_string()));
        assert_eq!(headers.cookie, Some("session=abc".to_string()));

        assert_eq!(config.resources.len(), 1);
        let resource = &config.resources[0];
        assert_eq!(resource.url.to_string(), "https://example.com/image.webp");
        assert_eq!(resource.name, Some("image".to_string()));
        assert_eq!(resource.priority, Some(10));

        let dest = resource.dest.as_ref().expect("dest should be present");
        assert_eq!(
            dest.provider
                .as_ref()
                .expect("provider should be present")
                .to_string(),
            "local"
        );
        assert_eq!(
            dest.path.as_ref().expect("path should be present"),
            "~/downloads"
        );

        let resource_config = resource.config.as_ref().expect("config should be present");
        assert_eq!(resource_config.quality, Some(95));
    }

    #[test]
    fn test_yaml_config_minimal() {
        let config: IngestionConfig =
            serde_yaml::from_str(YAML_MINIMAL).expect("Failed to parse YAML");

        assert!(config.default_dest.is_some());
        assert!(config.default_dest.as_ref().unwrap().provider.is_none());
        assert!(config.default_dest.as_ref().unwrap().path.is_none());
        assert!(config.priority.is_none());
        assert!(config.chunk_size.is_none());
        assert!(config.headers.is_none());

        assert_eq!(config.resources.len(), 1);
        let resource = &config.resources[0];
        assert_eq!(resource.url.to_string(), "https://example.com/file.txt");
        assert!(resource.name.is_none());
        assert!(resource.priority.is_none());
        assert!(resource.dest.is_none());
        assert!(resource.config.is_none());
    }

    #[test]
    fn test_yaml_config_multiple_resources() {
        let config: IngestionConfig =
            serde_yaml::from_str(YAML_MULTIPLE_RESOURCES).expect("Failed to parse YAML");

        assert_eq!(config.resources.len(), 3);

        assert_eq!(config.resources[0].name, Some("first_image".to_string()));
        assert_eq!(config.resources[0].priority, Some(10));

        assert_eq!(config.resources[1].name, Some("second_image".to_string()));
        assert!(config.resources[1].priority.is_none());

        assert_eq!(
            config.resources[2].url.to_string(),
            "https://example.com/image3.gif"
        );
        assert!(config.resources[2].name.is_none());
    }

    #[test]
    fn test_yaml_config_destination_defaults() {
        let config: IngestionConfig =
            serde_yaml::from_str(YAML_DEST).expect("Failed to parse YAML");

        let dest = config.resources[0]
            .dest
            .as_ref()
            .expect("dest should be present");
        assert!(dest.provider.is_none());
        assert_eq!(dest.path, Some("/custom/path".to_string()));
    }

    #[test]
    fn test_yaml_config_headers_only() {
        let config: IngestionConfig =
            serde_yaml::from_str(YAML_HEADERS).expect("Failed to parse YAML");

        let headers = config.headers.expect("headers should be present");
        assert_eq!(headers.authorization, Some("Bearer mytoken123".to_string()));
        assert!(headers.cookie.is_none());
    }

    #[test]
    fn test_yaml_config_resource_level_config() {
        let config: IngestionConfig =
            serde_yaml::from_str(YAML_RESOURCE_CONFIG).expect("Failed to parse YAML");

        let resource_config = config.resources[0]
            .config
            .as_ref()
            .expect("config should be present");
        assert_eq!(resource_config.quality, Some(80));
        assert!(resource_config.compression_override.is_none());
    }

    // --- config_test.rs AppConfig tests ---

    use crate::cli::{Cli, Commands, RunArgs};
    use clap::Parser;

    const TOML_DEFAULTS: &str = r#"
[scheduler]
file_workers = 5
chunk_workers = 20
max_pending_jobs = 10000
max_per_host = 2

[compression]
threshold_mb = 512
quality = 95

[storage]
default_provider = "local"
default_path = "~/downloads"
chunk_size = "128MB"
temp_dir = "/tmp/ingest"
"#;

    fn toml_defaults() -> crate::settings::TomlRawConfig {
        toml::from_str(TOML_DEFAULTS).unwrap()
    }

    const YAML_MINIMAL_NO_PROVIDER: &str = r#"
resources:
  - url: https://example.com/file.txt
"#;

    const YAML_WITH_PROVIDER: &str = r#"
provider: s3
path: /custom/path
resources:
  - url: https://example.com/file.txt
"#;

    const YAML_WITH_CHUNK_SIZE: &str = r#"
chunk_size: 256MB
resources:
  - url: https://example.com/file.txt
"#;

    fn default_run_args() -> RunArgs {
        let cli = Cli::try_parse_from(["ingest", "run", "test.yaml"]).unwrap();
        match cli.command {
            Commands::Run(a) => a,
            _ => panic!("expected Run"),
        }
    }

    #[test]
    fn test_app_config_uses_toml_defaults_when_yaml_omits_them() {
        let toml = toml_defaults();
        let yaml: IngestionConfig = serde_yaml::from_str(YAML_MINIMAL_NO_PROVIDER).unwrap();
        let args = default_run_args();

        let cfg = AppConfig::from_sources(&yaml, toml, args, "r://h".into(), "m://h".into());

        assert_eq!(cfg.default_provider, "local");
        assert_eq!(cfg.default_path, "~/downloads");
        assert_eq!(cfg.chunk_size, "128MB");
    }

    #[test]
    fn test_app_config_yaml_overrides_storage() {
        let toml = toml_defaults();
        let yaml: IngestionConfig = serde_yaml::from_str(YAML_WITH_PROVIDER).unwrap();
        let args = default_run_args();

        let cfg = AppConfig::from_sources(&yaml, toml, args, "r://h".into(), "m://h".into());

        assert_eq!(cfg.default_provider, "s3");
        assert_eq!(cfg.default_path, "/custom/path");
    }

    #[test]
    fn test_app_config_yaml_chunk_size_overrides_toml() {
        let toml = toml_defaults();
        let yaml: IngestionConfig = serde_yaml::from_str(YAML_WITH_CHUNK_SIZE).unwrap();
        let args = default_run_args();

        let cfg = AppConfig::from_sources(&yaml, toml, args, "r://h".into(), "m://h".into());

        assert_eq!(cfg.chunk_size, "256MB");
    }

    #[test]
    fn test_app_config_cli_priority_overrides_yaml() {
        let toml = toml_defaults();
        let yaml: IngestionConfig = serde_yaml::from_str(YAML_WITH_PROVIDER).unwrap();
        let cli = Cli::try_parse_from(["ingest", "run", "test.yaml", "--priority", "99"]).unwrap();
        let args = match cli.command {
            Commands::Run(a) => a,
            _ => panic!("expected Run"),
        };

        let cfg = AppConfig::from_sources(&yaml, toml, args, "r://h".into(), "m://h".into());

        assert_eq!(cfg.priority, 99);
    }

    #[test]
    fn test_app_config_cli_workers_overrides_toml() {
        let toml = toml_defaults();
        let yaml: IngestionConfig = serde_yaml::from_str(YAML_MINIMAL_NO_PROVIDER).unwrap();
        let cli = Cli::try_parse_from(["ingest", "run", "test.yaml", "--workers", "42"]).unwrap();
        let args = match cli.command {
            Commands::Run(a) => a,
            _ => panic!("expected Run"),
        };

        let cfg = AppConfig::from_sources(&yaml, toml, args, "r://h".into(), "m://h".into());

        assert_eq!(cfg.file_workers, 42);
    }

    #[test]
    fn test_app_config_default_priority() {
        let toml = toml_defaults();
        let yaml: IngestionConfig = serde_yaml::from_str(YAML_MINIMAL_NO_PROVIDER).unwrap();
        let args = default_run_args();

        let cfg = AppConfig::from_sources(&yaml, toml, args, "r://h".into(), "m://h".into());

        assert_eq!(cfg.priority, 0);
    }

    const YAML_WITH_COMPRESSION: &str = r#"
compression_override: webp
resources:
  - url: https://example.com/file.txt
"#;

    #[test]
    fn test_app_config_yaml_compression_override() {
        let toml = toml_defaults();
        let yaml: IngestionConfig = serde_yaml::from_str(YAML_WITH_COMPRESSION).unwrap();
        let args = default_run_args();

        let cfg = AppConfig::from_sources(&yaml, toml, args, "r://h".into(), "m://h".into());

        assert!(cfg.compression_override.is_some());
    }

    const YAML_WITH_HEADERS: &str = r#"
headers:
  authorization: Bearer test-token
  cookie: session=abc
resources:
  - url: https://example.com/file.txt
"#;

    #[test]
    fn test_app_config_yaml_headers() {
        let toml = toml_defaults();
        let yaml: IngestionConfig = serde_yaml::from_str(YAML_WITH_HEADERS).unwrap();
        let args = default_run_args();

        let cfg = AppConfig::from_sources(&yaml, toml, args, "r://h".into(), "m://h".into());

        let headers = cfg.headers.expect("headers should be set from YAML");
        assert_eq!(headers.authorization, Some("Bearer test-token".into()));
        assert_eq!(headers.cookie, Some("session=abc".into()));
    }

    #[test]
    fn test_app_config_yaml_no_compression_no_headers() {
        let toml = toml_defaults();
        let yaml: IngestionConfig = serde_yaml::from_str(YAML_MINIMAL_NO_PROVIDER).unwrap();
        let args = default_run_args();

        let cfg = AppConfig::from_sources(&yaml, toml, args, "r://h".into(), "m://h".into());

        assert!(cfg.compression_override.is_none());
        assert!(cfg.headers.is_none());
        assert_eq!(cfg.quality, Some(95));
    }

    const YAML_WITH_QUALITY: &str = r#"
quality: 80
resources:
  - url: https://example.com/file.txt
"#;

    #[test]
    fn test_app_config_yaml_quality() {
        let toml = toml_defaults();
        let yaml: IngestionConfig = serde_yaml::from_str(YAML_WITH_QUALITY).unwrap();
        let args = default_run_args();

        let cfg = AppConfig::from_sources(&yaml, toml, args, "r://h".into(), "m://h".into());

        assert_eq!(cfg.quality, Some(80));
    }
}
