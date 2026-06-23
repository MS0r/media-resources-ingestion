use std::path::PathBuf;

use crate::{
    config::{EnqueueConfig, RunConfig},
    settings::TomlRawConfig,
    storage::Provider,
};
use mongodb::bson::DateTime as MongoDateTime;
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
    pub offset_start: u64,
    pub offset_end: u64,
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
    Gzip,
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
    /// Source authentication provider: "auto" | "gdrive" | "dropbox" | "s3" | "headers" | "none"
    /// "auto" = detect from URL (default). "headers" / "none" = use static headers only.
    #[serde(default)]
    pub source_auth: Option<String>,
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
    /// Default source_auth for all resources (overridable per-resource).
    #[serde(default)]
    source_auth: Option<String>,
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

    // Compression + headers + quality + source_auth (YAML, merged in from_sources)
    pub compression_override: Option<CompressionOverride>,
    pub headers: Option<Headers>,
    pub quality: Option<u8>,
    pub source_auth: Option<String>,

    // Run behavior (CLI, YAML fallback)
    pub yaml_path: PathBuf,
    pub priority: i32,
    pub dry_run: bool,
    pub follow: bool,
    pub output: OutputFormat,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OutputFormat {
    Table,
    Json,
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

/// Simple filter enum used for querying job lists by status.
/// This is distinct from `handlers::jobs::JobStatus` which carries runtime metadata.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobStatusFilter {
    Pending,
    Running,
    Completed,
    Failed,
    Retrying,
    Cancelled,
}

pub fn load_config(path: &PathBuf) -> Result<IngestionConfig, crate::error::ToolError> {
    let content = std::fs::read_to_string(path)?;
    let request: IngestionConfig = match serde_yaml::from_str(&content) {
        Ok(config) => config,
        Err(e) => {
            tracing::error!("YAML parse error: {}", e);
            return Err(e.into());
        }
    };

    Ok(request)
}

pub fn extract_run_config(
    yaml_config: IngestionConfig,
    toml_config: TomlRawConfig,
    run_args: RunConfig,
    redis_uri: String,
    mongo_uri: String,
) -> Result<(AppConfig, Vec<Resource>), crate::error::ToolError> {
    let config = AppConfig::from_sources(&yaml_config, toml_config, run_args, redis_uri, mongo_uri);
    let resources = yaml_config.resources;
    Ok((config, resources))
}

pub fn extract_enqueue_config(
    yaml_config: IngestionConfig,
    toml_config: TomlRawConfig,
    enqueue_args: EnqueueConfig,
    redis_uri: String,
    mongo_uri: String,
) -> Result<(AppConfig, Vec<Resource>), crate::error::ToolError> {
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
        args: RunConfig,
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
            source_auth: yaml.source_auth.clone(),
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
            source_auth: None,
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
        args: EnqueueConfig,
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
            source_auth: yaml.source_auth.clone(),
            yaml_path: args.yaml_path.clone(),
            priority,
            dry_run: args.dry_run,
            follow: false,
            output: args.output,
        }
    }
}

pub fn load_env_uris() -> Result<(String, String), crate::error::ToolError> {
    let redis_uri = std::env::var("REDIS_URI")?;
    let mongo_uri = std::env::var("MONGODB_URI")?;
    Ok((redis_uri, mongo_uri))
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

    #[test]
    fn test_resource_default_id() {
        let resource: Resource = serde_yaml::from_str("url: https://example.com/f.png").unwrap();
        assert!(!resource.id.is_empty());
    }

    #[test]
    fn test_resource_with_id() {
        let resource: Resource =
            serde_yaml::from_str("id: my_custom_id\nurl: https://example.com/f.png").unwrap();
        assert_eq!(resource.id, "my_custom_id");
    }

    #[test]
    fn test_resource_priority_roundtrip() {
        let resource: Resource = serde_yaml::from_str(
            r#"
        id: r1
        url: https://example.com/f.png
        priority: 5
        "#,
        )
        .unwrap();
        assert_eq!(resource.priority, Some(5));

        let deser = serde_yaml::to_value(&resource).unwrap();
        assert_eq!(deser.get("priority").and_then(|v| v.as_i64()), Some(5));
    }

    #[test]
    fn test_headers_serde() {
        let json = r#"{"authorization":"Bearer xyz","cookie":"session=abc"}"#;
        let h: Headers = serde_json::from_str(json).unwrap();
        assert_eq!(h.authorization.unwrap(), "Bearer xyz");
        assert_eq!(h.cookie.unwrap(), "session=abc");
    }

    #[test]
    fn test_resource_inherits_config_when_none() {
        let resource: Resource = serde_yaml::from_str("url: https://example.com/f.png").unwrap();
        assert!(resource.config.is_none());
    }

    #[test]
    fn test_resource_without_compression_override() {
        let resource: Resource = serde_yaml::from_str(
            r#"
        url: https://example.com/f.png
        config:
          quality: 80
        "#,
        )
        .unwrap();
        let resource_config = resource.config.unwrap();
        assert_eq!(resource_config.quality, Some(80));
        assert!(resource_config.compression_override.is_none());
    }

    // --- config_test.rs AppConfig tests ---

    use crate::config::RunConfig;

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

    fn default_run_config() -> RunConfig {
        RunConfig {
            yaml_path: PathBuf::from("test.yaml"),
            dry_run: false,
            priority: None,
            workers: None,
            follow: false,
            no_follow: false,
            output: OutputFormat::Table,
        }
    }

    #[test]
    fn test_app_config_uses_toml_defaults_when_yaml_omits_them() {
        let yaml: IngestionConfig = serde_yaml::from_str(YAML_MINIMAL_NO_PROVIDER).unwrap();
        let config = AppConfig::from_sources(
            &yaml,
            toml_defaults(),
            default_run_config(),
            "redis://localhost".into(),
            "mongodb://localhost".into(),
        );

        assert_eq!(config.file_workers, 5);
        assert_eq!(config.chunk_workers, 20);
        assert_eq!(config.compression_threshold_mb, 512);
        assert_eq!(config.compression_quality, 95);
        assert_eq!(config.default_provider, "local");
        assert_eq!(config.chunk_size, "128MB");
        assert_eq!(config.temp_dir, "/tmp/ingest");
    }

    #[test]
    fn test_app_config_yaml_overrides_toml_defaults() {
        let yaml: IngestionConfig = serde_yaml::from_str(YAML_WITH_PROVIDER).unwrap();
        let config = AppConfig::from_sources(
            &yaml,
            toml_defaults(),
            default_run_config(),
            "redis://localhost".into(),
            "mongodb://localhost".into(),
        );

        assert_eq!(config.default_provider, "s3");
        assert_eq!(config.default_path, "/custom/path");
    }

    #[test]
    fn test_app_config_yaml_chunk_size_overrides_toml() {
        let yaml: IngestionConfig = serde_yaml::from_str(YAML_WITH_CHUNK_SIZE).unwrap();
        let config = AppConfig::from_sources(
            &yaml,
            toml_defaults(),
            default_run_config(),
            "redis://localhost".into(),
            "mongodb://localhost".into(),
        );

        assert_eq!(config.chunk_size, "256MB");
    }

    #[test]
    fn test_app_config_extract_run_config_resources() {
        let yaml: IngestionConfig = serde_yaml::from_str(YAML_MINIMAL_NO_PROVIDER).unwrap();
        let (config, resources) = extract_run_config(
            yaml,
            toml_defaults(),
            default_run_config(),
            "redis://localhost".into(),
            "mongodb://localhost".into(),
        )
        .unwrap();
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].url.as_str(), "https://example.com/file.txt");
        assert_eq!(config.priority, 0);
    }

    #[test]
    fn test_app_config_priority_from_yaml() {
        let yaml: IngestionConfig = serde_yaml::from_str(
            r#"
        priority: 10
        resources:
          - url: https://example.com/file.txt
        "#,
        )
        .unwrap();
        let config = AppConfig::from_sources(
            &yaml,
            toml_defaults(),
            default_run_config(),
            "redis://localhost".into(),
            "mongodb://localhost".into(),
        );
        assert_eq!(config.priority, 10);
    }

    #[test]
    fn test_app_config_priority_from_run_config() {
        let yaml: IngestionConfig = serde_yaml::from_str(YAML_MINIMAL_NO_PROVIDER).unwrap();
        let run_cfg = RunConfig {
            priority: Some(42),
            ..default_run_config()
        };
        let config = AppConfig::from_sources(
            &yaml,
            toml_defaults(),
            run_cfg,
            "redis://localhost".into(),
            "mongodb://localhost".into(),
        );
        assert_eq!(config.priority, 42);
    }

    #[test]
    fn test_app_config_priority_cli_overrides_yaml() {
        let yaml: IngestionConfig = serde_yaml::from_str(
            r#"
        priority: 10
        resources:
          - url: https://example.com/file.txt
        "#,
        )
        .unwrap();
        let run_cfg = RunConfig {
            priority: Some(99),
            ..default_run_config()
        };
        let config = AppConfig::from_sources(
            &yaml,
            toml_defaults(),
            run_cfg,
            "redis://localhost".into(),
            "mongodb://localhost".into(),
        );
        assert_eq!(config.priority, 99);
    }

    #[test]
    fn test_app_config_workers_from_run_config() {
        let yaml: IngestionConfig = serde_yaml::from_str(YAML_MINIMAL_NO_PROVIDER).unwrap();
        let run_cfg = RunConfig {
            workers: Some(10),
            ..default_run_config()
        };
        let config = AppConfig::from_sources(
            &yaml,
            toml_defaults(),
            run_cfg,
            "redis://localhost".into(),
            "mongodb://localhost".into(),
        );
        assert_eq!(config.file_workers, 10);
    }

    #[test]
    fn test_app_config_follow_default() {
        let yaml: IngestionConfig = serde_yaml::from_str(YAML_MINIMAL_NO_PROVIDER).unwrap();
        let config = AppConfig::from_sources(
            &yaml,
            toml_defaults(),
            default_run_config(),
            "redis://localhost".into(),
            "mongodb://localhost".into(),
        );
        assert!(config.follow);
    }

    #[test]
    fn test_app_config_follow_enabled() {
        let yaml: IngestionConfig = serde_yaml::from_str(YAML_MINIMAL_NO_PROVIDER).unwrap();
        let run_cfg = RunConfig {
            follow: true,
            no_follow: false,
            ..default_run_config()
        };
        let config = AppConfig::from_sources(
            &yaml,
            toml_defaults(),
            run_cfg,
            "redis://localhost".into(),
            "mongodb://localhost".into(),
        );
        assert!(config.follow);
    }

    #[test]
    fn test_app_config_no_follow_disables_follow() {
        let yaml: IngestionConfig = serde_yaml::from_str(YAML_MINIMAL_NO_PROVIDER).unwrap();
        let run_cfg = RunConfig {
            follow: false,
            no_follow: true,
            ..default_run_config()
        };
        let config = AppConfig::from_sources(
            &yaml,
            toml_defaults(),
            run_cfg,
            "redis://localhost".into(),
            "mongodb://localhost".into(),
        );
        assert!(!config.follow);
    }

    #[test]
    fn test_app_config_dry_run() {
        let yaml: IngestionConfig = serde_yaml::from_str(YAML_MINIMAL_NO_PROVIDER).unwrap();
        let run_cfg = RunConfig {
            dry_run: true,
            ..default_run_config()
        };
        let config = AppConfig::from_sources(
            &yaml,
            toml_defaults(),
            run_cfg,
            "redis://localhost".into(),
            "mongodb://localhost".into(),
        );
        assert!(config.dry_run);
    }

    #[test]
    fn test_headers_deserialize_lowercase() {
        let yaml: IngestionConfig = serde_yaml::from_str(
            r#"
        headers:
          authorization: Bearer xyz
        resources:
          - url: https://example.com/f.png
        "#,
        )
        .unwrap();
        assert_eq!(yaml.headers.unwrap().authorization.unwrap(), "Bearer xyz");
    }

    #[test]
    fn test_load_env_uris_fails_when_not_set() {
        // Remove the env vars for this test
        unsafe {
            std::env::remove_var("REDIS_URI");
            std::env::remove_var("MONGODB_URI");
        }
        let result = load_env_uris();
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_chunk_size_mb() {
        let _s = "128MB".to_string();
        // parse_chunk_size is not exported from models, but we can test via config
        // This is covered in execute.rs tests
    }

    #[test]
    fn test_serde_output_format() {
        let json = serde_json::to_string(&OutputFormat::Json).unwrap();
        assert_eq!(json, "\"json\"");
        let table: OutputFormat = serde_json::from_str("\"table\"").unwrap();
        assert_eq!(table, OutputFormat::Table);
    }

    #[test]
    fn test_serde_job_status_filter() {
        for (input, expected) in [
            ("pending", JobStatusFilter::Pending),
            ("running", JobStatusFilter::Running),
            ("completed", JobStatusFilter::Completed),
            ("failed", JobStatusFilter::Failed),
            ("retrying", JobStatusFilter::Retrying),
            ("cancelled", JobStatusFilter::Cancelled),
        ] {
            let json = format!("\"{}\"", input);
            let deser: JobStatusFilter = serde_json::from_str(&json).unwrap();
            assert_eq!(deser, expected, "failed for {}", input);
        }
    }
}
