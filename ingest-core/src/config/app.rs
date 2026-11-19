//! Runtime application config: the merged, fully-resolved configuration
//! the scheduler and worker actually use.
//!
//! Built from three sources (in priority order, high → low):
//! 1. CLI flags — `RunConfig`
//! 2. YAML file — `IngestionConfig`
//! 3. TOML file — `TomlRawConfig`
//!
//! See [`AppConfig::from_sources`] for the exact merge rules.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::{
    compression::CompressionOverride,
    config::{
        ingest_yaml::{IngestionConfig, Resource},
        run::RunConfig,
        toml::TomlRawConfig,
    },
    domain::{Headers, SourceAuth},
    error::ToolError,
};

/// The fully-resolved runtime config shared across the worker, scheduler,
/// and gRPC handlers. Cloned cheaply (every field is `Copy`, `String`, or
/// small `Vec`/`Option`).
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

    // Mongo pool sizing (TOML)
    pub mongo_pool_min: u32,
    pub mongo_pool_max: u32,

    // Sharding (TOML + derived)
    pub shard_count: u32,
    pub worker_id: u32,

    // Shutdown (TOML)
    pub shutdown_grace_secs: u64,

    // Compression + headers + quality + source_auth (YAML, merged in from_sources)
    pub compression_override: Option<CompressionOverride>,
    pub headers: Option<Headers>,
    pub quality: Option<u8>,
    pub source_auth: Option<SourceAuth>,

    // Run behavior (CLI, YAML fallback)
    pub yaml_path: PathBuf,
    pub priority: i32,
    pub dry_run: bool,
    pub follow: bool,
    pub output: OutputFormat,
}

/// How CLI output is rendered. The CLI picks one of these based on a flag.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OutputFormat {
    Table,
    Json,
}

impl AppConfig {
    /// Merge CLI + YAML + TOML into a single `AppConfig`.
    ///
    /// Merge rules:
    /// - `priority`: `cli.priority ?? yaml.priority ?? 0`
    /// - `quality`: `yaml.quality ?? Some(toml.compression.quality)`
    /// - `file_workers`: `cli.workers ?? toml.scheduler.file_workers`
    /// - `default_provider`/`default_path`: yaml overrides toml when present
    /// - `chunk_size`: yaml overrides toml when present
    /// - `follow`: `cli.follow || !cli.no_follow`
    /// - YAML-only: `compression_override`, `headers`, `source_auth`
    /// - everything else: TOML
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
            mongo_pool_min: toml.scheduler.mongo_pool_min,
            mongo_pool_max: toml.scheduler.mongo_pool_max,
            shard_count: toml.scheduler.shard_count,
            worker_id: 0,
            shutdown_grace_secs: toml.scheduler.shutdown_grace_secs,
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

    /// Build an `AppConfig` from just TOML — used by the gRPC server and
    /// standalone worker, where there's no YAML/CLI input.
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
            mongo_pool_min: toml.scheduler.mongo_pool_min,
            mongo_pool_max: toml.scheduler.mongo_pool_max,
            shard_count: toml.scheduler.shard_count,
            worker_id: 0,
            shutdown_grace_secs: toml.scheduler.shutdown_grace_secs,
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
}

/// Read `REDIS_URI` and `MONGODB_URI` from the environment.
pub fn load_env_uris() -> Result<(String, String), ToolError> {
    let redis_uri = std::env::var("REDIS_URI")?;
    let mongo_uri = std::env::var("MONGODB_URI")?;
    Ok((redis_uri, mongo_uri))
}

/// Combine the three config inputs into `(AppConfig, Vec<Resource>)`.
/// Convenience used by the CLI to avoid threading the merge logic
/// through every subcommand.
pub fn extract_run_config(
    yaml_config: IngestionConfig,
    toml_config: TomlRawConfig,
    run_args: RunConfig,
    redis_uri: String,
    mongo_uri: String,
) -> Result<(AppConfig, Vec<Resource>), ToolError> {
    let config = AppConfig::from_sources(&yaml_config, toml_config, run_args, redis_uri, mongo_uri);
    let resources = yaml_config.resources;
    Ok((config, resources))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ingest_yaml::load_config;
    use std::path::PathBuf;

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

    fn toml_defaults() -> TomlRawConfig {
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
    fn test_serde_output_format() {
        let json = serde_json::to_string(&OutputFormat::Json).unwrap();
        assert_eq!(json, "\"json\"");
        let table: OutputFormat = serde_json::from_str("\"table\"").unwrap();
        assert_eq!(table, OutputFormat::Table);
    }

    // Used so `load_config` import is kept in scope; the function is also
    // re-exported through `ingest_core::models::load_config`.
    #[test]
    fn test_load_config_is_callable() {
        let tmp = std::env::temp_dir().join("app-load-config-test.yaml");
        std::fs::write(&tmp, "resources:\n  - url: https://example.com/f.png\n").unwrap();
        let cfg = load_config(&tmp).unwrap();
        assert_eq!(cfg.resources.len(), 1);
        std::fs::remove_file(&tmp).ok();
    }
}
