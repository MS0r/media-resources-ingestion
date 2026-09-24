//! Configuration types — the three layered config sources.
//!
//! - [`ingest_yaml`] — the user-facing YAML file (`IngestionConfig`, `Resource`, …)
//! - [`toml`] — the on-disk `.ingest/config.toml` (`TomlRawConfig`, `SchedulerConfig`, …)
//! - [`app`] — the merged runtime config (`AppConfig`, `OutputFormat`)
//! - [`run`] — CLI args that override the YAML at enqueue time (`RunConfig`)

pub mod app;
pub mod ingest_yaml;
pub mod run;
pub mod toml;

pub use app::{AppConfig, OutputFormat, extract_run_config, load_env_uris};
pub use ingest_yaml::{Destination, IngestionConfig, Resource, ResourceLevelConfig, load_config};
pub use run::RunConfig;
pub use toml::{TomlRawConfig, load_toml};
