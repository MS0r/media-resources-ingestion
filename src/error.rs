use bb8::RunError as bb8_error;
use bb8_mongodb::Error as bb8_mongodb_error;
use mongodb::bson::error::Error as bson_error;
use mongodb::error::Error as mongodb_error;
use redis::RedisError as redis_error;
use serde_json::Error as json_error;
use std::io::Error as io_error;
use thiserror::Error;
use toml::de::Error as toml_error;

use crate::storage::DynError;

#[derive(Error, Debug)]
pub enum ToolError {
    #[error("Redis error: {0}")]
    RedisError(#[from] redis_error),
    #[error("MongoDB error: {0}")]
    MongoError(#[from] mongodb_error),
    #[error("MongoDB pool error: {0}")]
    MongoPoolError(#[from] bb8_error<bb8_mongodb_error>),
    #[error("MongoDB connection error: {0}")]
    MongoConnectionError(#[from] bb8_mongodb_error),
    #[error("BSON error: {0}")]
    BsonError(#[from] bson_error),
    #[error("Config parse error: {0}")]
    ConfigParseError(#[from] toml_error),
    #[error("JSON error: {0}")]
    JsonError(#[from] json_error),
    #[error("I/O error: {0}")]
    IoError(#[from] io_error),
    #[error("{0}")]
    Message(String),
    #[error("Configuration error: {0}")]
    ConfigError(String),
    #[error("Validation error: {0}")]
    ValidationError(String),
    #[error("Auth error: {0}")]
    AuthError(String),
    #[error("Job execution error: {0}")]
    JobExecutionError(String),
    #[error("Interrupted by SIGINT (Ctrl+C)")]
    Interrupted,
}

impl From<String> for ToolError {
    fn from(s: String) -> Self {
        ToolError::Message(s)
    }
}

impl ToolError {
    pub fn exit_code(&self) -> i32 {
        match self {
            ToolError::ConfigError(_)
            | ToolError::ValidationError(_)
            | ToolError::ConfigParseError(_) => 2,
            ToolError::RedisError(_)
            | ToolError::MongoError(_)
            | ToolError::MongoPoolError(_)
            | ToolError::MongoConnectionError(_)
            | ToolError::BsonError(_) => 3,
            ToolError::AuthError(_) => 4,
            ToolError::Interrupted => 130,
            ToolError::JsonError(_)
            | ToolError::IoError(_)
            | ToolError::Message(_)
            | ToolError::JobExecutionError(_) => 1,
        }
    }
}

#[derive(Error, Debug)]
pub enum JobErrorOutcome {
    #[error("Retryable: {0}")]
    Retryable(String),
    #[error("Fatal: {0}")]
    Fatal(String),
}

#[derive(Error, Debug)]
pub enum JobError {
    #[error("Network error: {0}")]
    WreqError(#[from] wreq::Error),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Image error {0}")]
    ImageError(#[from] image::ImageError),
    #[error("Join error {0}")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("Other retryable error: {0}")]
    OtherRetryable(String),
    #[error("Fatal error: {0}")]
    OtherFatal(String),
}

impl From<ToolError> for JobErrorOutcome {
    fn from(e: ToolError) -> Self {
        match e {
            ToolError::RedisError(_)
            | ToolError::MongoError(_)
            | ToolError::MongoPoolError(_)
            | ToolError::MongoConnectionError(_)
            | ToolError::BsonError(_)
            | ToolError::IoError(_)
            | ToolError::JsonError(_)
            | ToolError::Message(_)
            | ToolError::JobExecutionError(_) => JobErrorOutcome::Retryable(e.to_string()),
            ToolError::ConfigError(_)
            | ToolError::ConfigParseError(_)
            | ToolError::ValidationError(_)
            | ToolError::AuthError(_)
            | ToolError::Interrupted => JobErrorOutcome::Fatal(e.to_string()),
        }
    }
}

impl From<JobError> for JobErrorOutcome {
    fn from(e: JobError) -> Self {
        match e {
            JobError::WreqError(_)
            | JobError::IoError(_)
            | JobError::OtherRetryable(_)
            | JobError::JoinError(_) => JobErrorOutcome::Retryable(e.to_string()),
            JobError::ImageError(_) | JobError::OtherFatal(_) => {
                JobErrorOutcome::Fatal(e.to_string())
            }
        }
    }
}

impl From<DynError> for JobErrorOutcome {
    fn from(e: DynError) -> Self {
        JobErrorOutcome::Retryable(e.to_string())
    }
}
