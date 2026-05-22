use std::path::PathBuf;

use crate::{settings::TomlConfig, storage::Provider};
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

pub struct MainConfig {
    pub toml_config: TomlConfig,
    pub yaml_config: IngestionConfig,
    pub yaml_path: PathBuf,
    pub redis_uri: String,
    pub mongo_uri: String,
}

fn default_uuid() -> String {
    uuid::Uuid::new_v4().to_string()
}
