use bb8::RunError as bb8_error;
use bb8_mongodb::Error as bb8_mongodb_error;
use mongodb::{bson::error::Error as bson_error, error::Error as mongodb_error};
use redis::RedisError as redis_error;
use serde_json::Error as json_error;
use std::{env::VarError as env_error, io::Error as io_error};
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
    #[error("Environment variable error {0}")]
    EnvError(#[from] env_error),
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
            | ToolError::ConfigParseError(_)
            | ToolError::EnvError(_) => 2,
            ToolError::RedisError(_)
            | ToolError::MongoError(_)
            | ToolError::MongoPoolError(_)
            | ToolError::MongoConnectionError(_)
            | ToolError::BsonError(_) => 3,
            ToolError::AuthError(_) => 4,
            ToolError::Interrupted => 130,
            _ => 1,
        }
    }
}

#[derive(Error, Debug)]
pub enum JobError {
    #[error("Wreq HTTP error: {0}")]
    WreqError(#[from] wreq::Error),
    #[error("I/O error: {0}")]
    IoError(#[from] io_error),
    #[error("Image processing error: {0}")]
    ImageError(#[from] image::ImageError),
    #[error("Join error: {0}")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("{0}")]
    OtherRetryable(String),
    #[error("{0}")]
    OtherFatal(String),
}

#[derive(Debug, Clone)]
pub enum JobErrorOutcome {
    Retryable(String),
    Fatal(String),
}

impl std::fmt::Display for JobErrorOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobErrorOutcome::Retryable(msg) => write!(f, "Retryable: {msg}"),
            JobErrorOutcome::Fatal(msg) => write!(f, "Fatal: {msg}"),
        }
    }
}

impl From<std::io::Error> for JobErrorOutcome {
    fn from(e: std::io::Error) -> Self {
        JobErrorOutcome::Retryable(e.to_string())
    }
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
            | ToolError::EnvError(_)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_toml_error() -> toml::de::Error {
        toml::from_str::<toml::value::Value>("invalid toml [[[").unwrap_err()
    }

    fn make_mongo_error() -> mongodb::error::Error {
        mongodb::error::Error::custom(std::io::Error::new(std::io::ErrorKind::Other, "test"))
    }

    #[test]
    fn test_exit_code_config() {
        assert_eq!(ToolError::ConfigError("bad".into()).exit_code(), 2);
        assert_eq!(ToolError::ValidationError("bad".into()).exit_code(), 2);
        assert_eq!(
            ToolError::ConfigParseError(make_toml_error()).exit_code(),
            2
        );
    }

    #[test]
    fn test_exit_code_auth() {
        assert_eq!(ToolError::AuthError("denied".into()).exit_code(), 4);
    }

    #[test]
    fn test_exit_code_backend() {
        assert_eq!(
            ToolError::RedisError(redis::RedisError::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                "conn"
            )))
            .exit_code(),
            3
        );
        assert_eq!(ToolError::MongoError(make_mongo_error()).exit_code(), 3);
    }

    #[test]
    fn test_exit_code_job_failure() {
        assert_eq!(ToolError::JobExecutionError("fail".into()).exit_code(), 1);
        assert_eq!(
            ToolError::IoError(std::io::Error::new(std::io::ErrorKind::Other, "fail")).exit_code(),
            1
        );
        assert_eq!(ToolError::Message("msg".into()).exit_code(), 1);
        assert_eq!(
            ToolError::JsonError(serde_json::from_str::<()>("").unwrap_err()).exit_code(),
            1
        );
    }

    #[test]
    fn test_exit_code_interrupted() {
        assert_eq!(ToolError::Interrupted.exit_code(), 130);
    }

    #[test]
    fn test_error_outcome_from_tool_error() {
        let outcome: JobErrorOutcome =
            ToolError::IoError(std::io::Error::new(std::io::ErrorKind::Other, "retry")).into();
        assert!(matches!(outcome, JobErrorOutcome::Retryable(_)));

        let outcome: JobErrorOutcome = ToolError::ConfigError("fatal".into()).into();
        assert!(matches!(outcome, JobErrorOutcome::Fatal(_)));
    }

    #[test]
    fn test_error_outcome_from_job_error() {
        let outcome: JobErrorOutcome = JobError::OtherRetryable("retry me".into()).into();
        assert!(matches!(outcome, JobErrorOutcome::Retryable(_)));

        let outcome: JobErrorOutcome = JobError::OtherFatal("give up".into()).into();
        assert!(matches!(outcome, JobErrorOutcome::Fatal(_)));
    }
}
