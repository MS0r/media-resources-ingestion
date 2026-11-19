#![allow(dead_code)]

// Auth + bootstrap + handlers/services form the executable backbone.
pub mod auth;
pub(crate) mod bootstrap;
pub mod compression;
pub mod config;
pub mod domain;
pub mod error;
pub mod grpc;
pub mod handlers;
pub mod job;
pub mod models;
pub(crate) mod providers;
pub mod services;
pub mod storage;
pub mod worker;

// Public API re-exports — this is what external crates (ingest-cli,
// ingest-server) rely on. Keep stable.
pub use bootstrap::enqueue;
pub use config::{RunConfig, TomlRawConfig};
pub use domain::{JobStatus, JobStatusFilter, SourceAuth};
pub use error::ToolError;
pub use job::{JobEffect, JobOutcome};
pub use models::{AppConfig, OutputFormat};
pub use services::mongo::MongoService;
pub use storage::Provider;
pub use worker::{Shutdown, Worker};
