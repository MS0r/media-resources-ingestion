//! YAML ingestion config — what the user-facing YAML file deserializes into.
//!
//! The shape:
//! ```yaml
//! provider: local               # optional default destination
//! path: /data                   # optional default destination
//! priority: 5                   # optional default
//! chunk_size: 128MB             # optional override
//! compression_override: gzip    # optional override
//! quality: 85                   # optional override
//! headers:                      # optional default headers
//!   authorization: Bearer xyz
//! source_auth: auto             # optional default source auth
//! resources:                    # required: list of files to ingest
//!   - id: my-file-1
//!     url: https://example.com/file.zip
//!     name: example
//!     priority: 10
//!     dest:                     # optional per-resource destination override
//!       provider: local
//!       path: /data/sub
//!     config:                   # optional per-resource config override
//!       compression_override: gzip
//!       quality: 80
//!       headers: { ... }
//!       source_auth: gdrive
//! ```

use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

use crate::{
    compression::CompressionOverride,
    domain::{Headers, SourceAuth},
    error::ToolError,
    storage::Provider,
};

/// Per-resource overrides that fall back to ingestion-level defaults
/// when fields are `None`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResourceLevelConfig {
    #[serde(default)]
    pub compression_override: Option<CompressionOverride>,
    pub quality: Option<u8>,
    #[serde(default)]
    pub headers: Option<Headers>,
    /// Source authentication strategy: "auto" | "gdrive" | "dropbox" | "s3" | "headers" | "none"
    /// "auto" = detect from URL (default). "headers" / "none" = use static headers only.
    #[serde(default)]
    pub source_auth: Option<SourceAuth>,
}

/// Where a resource should land. Provider may be absent and inherit
/// from the ingestion default; path is always optional.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Destination {
    #[serde(default)]
    pub provider: Option<Provider>,
    #[serde(default)]
    pub path: Option<String>,
}

/// A single resource to ingest. `id` is auto-generated when omitted.
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

fn default_uuid() -> String {
    Uuid::new_v4().to_string()
}

/// Top-level YAML config. The flat fields (`priority`, `chunk_size`,
/// `compression_override`, `quality`, `headers`, `source_auth`) are
/// ingestion-wide defaults that each `Resource` can override.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestionConfig {
    /// `provider` and `path` at the top level are flattened into a
    /// single optional `default_dest`. This lets users write:
    /// ```yaml
    /// provider: local
    /// path: /data
    /// resources: [...]
    /// ```
    #[serde(flatten)]
    pub default_dest: Option<Destination>,
    #[serde(default)]
    pub priority: Option<i32>,
    pub chunk_size: Option<String>,
    #[serde(default)]
    pub compression_override: Option<CompressionOverride>,
    pub quality: Option<u8>,
    #[serde(default)]
    pub headers: Option<Headers>,
    /// Default source_auth for all resources (overridable per-resource).
    #[serde(default)]
    pub source_auth: Option<SourceAuth>,
    pub resources: Vec<Resource>,
}

/// Read and parse a YAML config file from disk.
pub fn load_config(path: &std::path::PathBuf) -> Result<IngestionConfig, ToolError> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resource_default_id_is_uuid() {
        let resource: Resource = serde_yaml::from_str("url: https://example.com/f.png").unwrap();
        assert!(!resource.id.is_empty());
        Uuid::parse_str(&resource.id).expect("default id should be a uuid");
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

    #[test]
    fn test_ingestion_config_top_level_defaults() {
        let yaml = r#"
        provider: local
        path: /data
        priority: 5
        resources:
          - url: https://example.com/f.png
        "#;
        let cfg: IngestionConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.priority, Some(5));
        assert!(cfg.default_dest.is_some());
        let dest = cfg.default_dest.unwrap();
        assert_eq!(dest.provider, Some(Provider::Local));
        assert_eq!(dest.path.as_deref(), Some("/data"));
        assert_eq!(cfg.resources.len(), 1);
    }

    #[test]
    fn test_ingestion_config_headers_deserialize_lowercase() {
        let yaml = r#"
        headers:
          authorization: Bearer xyz
        resources:
          - url: https://example.com/f.png
        "#;
        let cfg: IngestionConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.headers.unwrap().authorization.unwrap(), "Bearer xyz");
    }

    #[test]
    fn test_load_config_invalid_yaml() {
        let tmp = std::env::temp_dir().join("invalid-ingest-yaml.yaml");
        std::fs::write(&tmp, "this is not: valid: yaml: [").unwrap();
        let result = load_config(&tmp);
        assert!(result.is_err());
        std::fs::remove_file(&tmp).ok();
    }
}
