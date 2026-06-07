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
        toml::from_str::<()>("invalid = [").unwrap_err()
    }

    fn make_mongo_error() -> mongodb::error::Error {
        mongodb::error::Error::from(std::io::Error::new(std::io::ErrorKind::Other, "no server"))
    }

    fn make_image_error() -> image::ImageError {
        image::ImageError::Unsupported(image::error::UnsupportedError::from_format_and_kind(
            image::error::ImageFormatHint::Name("unknown".into()),
            image::error::UnsupportedErrorKind::Format(image::error::ImageFormatHint::Name(
                "unknown".into(),
            )),
        ))
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
                "no server"
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
    fn test_tool_error_from_string() {
        let err: ToolError = String::from("something went wrong").into();
        assert!(matches!(err, ToolError::Message(s) if s == "something went wrong"));
    }

    #[test]
    fn test_tool_error_display() {
        let err = ToolError::ConfigError("missing field".into());
        assert!(err.to_string().contains("missing field"));
    }

    #[test]
    fn test_job_error_retryable_classification() {
        let io = JobError::IoError(std::io::Error::new(std::io::ErrorKind::Other, "timeout"));
        let outcome: JobErrorOutcome = io.into();
        assert!(matches!(outcome, JobErrorOutcome::Retryable(_)));

        let net = JobError::OtherRetryable("rate limited".into());
        let outcome: JobErrorOutcome = net.into();
        assert!(matches!(outcome, JobErrorOutcome::Retryable(_)));
    }

    #[test]
    fn test_job_error_fatal_classification() {
        let img = JobError::ImageError(make_image_error());
        let outcome: JobErrorOutcome = img.into();
        assert!(matches!(outcome, JobErrorOutcome::Fatal(_)));

        let fatal = JobError::OtherFatal("auth denied".into());
        let outcome: JobErrorOutcome = fatal.into();
        assert!(matches!(outcome, JobErrorOutcome::Fatal(_)));
    }

    #[test]
    fn test_tool_error_retryable_via_outcome() {
        let tool = ToolError::Message("retry".into());
        let outcome: JobErrorOutcome = tool.into();
        assert!(matches!(outcome, JobErrorOutcome::Retryable(_)));

        let tool = ToolError::JobExecutionError("server error".into());
        let outcome: JobErrorOutcome = tool.into();
        assert!(matches!(outcome, JobErrorOutcome::Retryable(_)));
    }

    #[test]
    fn test_tool_error_fatal_via_outcome() {
        let tool = ToolError::ConfigError("bad config".into());
        let outcome: JobErrorOutcome = tool.into();
        assert!(matches!(outcome, JobErrorOutcome::Fatal(_)));

        let tool = ToolError::AuthError("bad token".into());
        let outcome: JobErrorOutcome = tool.into();
        assert!(matches!(outcome, JobErrorOutcome::Fatal(_)));

        let tool = ToolError::ValidationError("invalid".into());
        let outcome: JobErrorOutcome = tool.into();
        assert!(matches!(outcome, JobErrorOutcome::Fatal(_)));

        let tool = ToolError::Interrupted;
        let outcome: JobErrorOutcome = tool.into();
        assert!(matches!(outcome, JobErrorOutcome::Fatal(_)));
    }

    #[test]
    fn test_job_error_outcome_display() {
        let r = JobErrorOutcome::Retryable("timeout".into());
        assert!(r.to_string().contains("timeout"));

        let f = JobErrorOutcome::Fatal("crash".into());
        assert!(f.to_string().contains("crash"));
    }
}
