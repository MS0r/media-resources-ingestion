use std::path::PathBuf;

use crate::{
    cli::{OutputFormat, RunArgs},
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
    pub file_hash: String, // SHA256 or similar
    pub original_url: Url,
    pub storage_provider: Provider,
    pub storage_path: String,
    pub original_file_size: u64,
    pub compressed_file_size: Option<u64>,
    pub compression_ratio: Option<f32>,
    pub mime_type: String,
    pub chunk_manifest: Option<Manifest>, // List of chunk identifiers if applicable
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
            compression_ratio: compressed_file_size.map(|c| c as f32 / original_file_size as f32),
            mime_type,
            chunk_manifest: None,
            upload_date: MongoDateTime::now(),
            duplicate_reference_count: 0,
            update_date: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub chunks: Vec<ChunkRef>,
    pub compression: Option<String>, // e.g. "image/webp"
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
    pub priority: Option<i32>,
    pub chunk_size: Option<String>,
    #[serde(default)]
    pub compression_override: Option<CompressionOverride>,
    #[serde(default)]
    pub headers: Option<Headers>,
    pub resources: Vec<Resource>,
}

fn default_uuid() -> String {
    uuid::Uuid::new_v4().to_string()
}

pub struct AppConfig {
    // Environment
    pub redis_uri: String,
    pub mongo_uri: String,

    // Scheduler (TOML, CLI --workers overrides file_workers)
    pub file_workers: usize,
    pub chunk_workers: usize,
    pub max_pending_jobs: usize,
    pub max_per_host: usize,

    // Compression (TOML)
    pub compression_threshold_mb: u64,
    pub compression_quality: u8,

    // Storage (TOML, YAML overrides provider/path/chunk_size)
    pub default_provider: String,
    pub default_path: String,
    pub chunk_size: String,
    pub temp_dir: String,

    // Run behavior (CLI, YAML fallback)
    pub yaml_path: PathBuf,
    pub priority: i32,
    pub dry_run: bool,
    pub follow: bool,
    pub output: OutputFormat,
}

impl AppConfig {
    pub fn from_sources(
        toml: TomlRawConfig,
        yaml: &IngestionConfig,
        args: &RunArgs,
        redis_uri: String,
        mongo_uri: String,
    ) -> Self {
        let default_provider = yaml
            .default_dest
            .as_ref()
            .and_then(|d| d.provider.as_ref().map(|p| p.to_string()))
            .unwrap_or(toml.storage.default_provider);
        let default_path = yaml
            .default_dest
            .as_ref()
            .and_then(|d| d.path.clone())
            .unwrap_or(toml.storage.default_path);
        let chunk_size = yaml.chunk_size.clone().unwrap_or(toml.storage.chunk_size);
        let priority = args.priority.or(yaml.priority).unwrap_or(0);
        let file_workers = args.workers.unwrap_or(toml.scheduler.file_workers);
        let follow = args.follow || !args.no_follow;

        Self {
            redis_uri,
            mongo_uri,
            file_workers,
            chunk_workers: toml.scheduler.chunk_workers,
            max_pending_jobs: toml.scheduler.max_pending_jobs,
            max_per_host: toml.scheduler.max_per_host,
            compression_threshold_mb: toml.compression.threshold_mb,
            compression_quality: toml.compression.quality,
            default_provider,
            default_path,
            chunk_size,
            temp_dir: toml.storage.temp_dir,
            yaml_path: args.yaml_path.clone(),
            priority,
            dry_run: args.dry_run,
            follow,
            output: args.output,
        }
    }
}
