use bb8::RunError as bb8_error;
use bb8_mongodb::Error as bb8_mongodb_error;
use mongodb::{bson::error::Error as bson_error, error::Error as mongodb_error};
use redis::RedisError as redis_error;
use serde_json::Error as json_error;
use serde_yaml::Error as serde_yaml_error;
use std::{env::VarError as env_error, io::Error as io_error};
use thiserror::Error;
use tokio::sync::AcquireError;
use toml::de::Error as toml_error;
use url::ParseError as url_parse_error;

use crate::storage::DynError;

/// Errors that occur when resolving source auth tokens (OAuth, S3 presigning).
#[derive(Error, Debug)]
pub enum AuthResolutionError {
    #[error("Source auth requires '{0}' but no auth registry configured")]
    NoRegistry(String),
    #[error("Source auth provider '{0}' not registered in auth registry")]
    Unregistered(String),
    #[error("Token refresh failed for '{provider}': {error}")]
    TokenRefresh { provider: String, error: String },
    #[error("Failed to generate S3 presigned URL: {0}")]
    S3Presign(String),
}

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
    #[error("YAML parse error: {0}")]
    YamlError(#[from] serde_yaml_error),
    #[error("JSON error: {0}")]
    JsonError(#[from] json_error),
    #[error("I/O error: {0}")]
    IoError(#[from] io_error),
    #[error("Environment variable error {0}")]
    EnvError(#[from] env_error),
    #[error("Wreq HTTP error: {0}")]
    WreqError(#[from] wreq::Error),
    #[error("URL parse error: {0}")]
    UrlParseError(#[from] url_parse_error),
    #[error("{0}")]
    Message(String),
    #[error("Configuration error: {0}")]
    ConfigError(String),
    #[error("Validation error: {0}")]
    ValidationError(String),
    #[error("Auth error: {0}")]
    AuthError(String),
    #[error("Auth resolution error: {0}")]
    AuthResolution(#[from] AuthResolutionError),
    #[error("Job execution error: {0}")]
    JobExecutionError(String),
    #[error("Semaphore acquisition failed: {0}")]
    SemaphoreError(#[from] AcquireError),
    #[error("Server error: {0}")]
    ServerError(#[from] tonic::transport::Error),
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
            | ToolError::YamlError(_)
            | ToolError::EnvError(_) => 2,
            ToolError::RedisError(_)
            | ToolError::MongoError(_)
            | ToolError::MongoPoolError(_)
            | ToolError::MongoConnectionError(_)
            | ToolError::BsonError(_) => 3,
            ToolError::AuthError(_) | ToolError::AuthResolution(_) => 4,
            ToolError::SemaphoreError(_) => 1,
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
    #[error("FFmpeg error: {0}")]
    FfmpegError(#[from] ffmpeg_next::Error),
    #[error("Zip error: {0}")]
    ZipError(#[from] zip::result::ZipError),
    #[error("7-Zip error: {0}")]
    SevenZError(#[from] sevenz_rust::Error),
    #[error("Channel send error: {0}")]
    SendError(#[from] tokio::sync::mpsc::error::SendError<ffmpeg_next::frame::Video>),
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
            | ToolError::WreqError(_)
            | ToolError::Message(_)
            | ToolError::JobExecutionError(_) => JobErrorOutcome::Retryable(e.to_string()),
            ToolError::ConfigError(_)
            | ToolError::ConfigParseError(_)
            | ToolError::YamlError(_)
            | ToolError::ValidationError(_)
            |             ToolError::AuthError(_)
            | ToolError::AuthResolution(_)
            | ToolError::EnvError(_)
            | ToolError::UrlParseError(_)
            | ToolError::Interrupted
            | ToolError::SemaphoreError(_)
            | ToolError::ServerError(_) => JobErrorOutcome::Fatal(e.to_string()),
        }
    }
}

impl From<JobError> for JobErrorOutcome {
    fn from(e: JobError) -> Self {
        match e {
            JobError::WreqError(_)
            | JobError::IoError(_)
            | JobError::OtherRetryable(_)
            | JobError::JoinError(_)
            | JobError::ZipError(_) => JobErrorOutcome::Retryable(e.to_string()),
            JobError::ImageError(_)
            | JobError::OtherFatal(_)
            | JobError::FfmpegError(_)
            | JobError::SevenZError(_)
            | JobError::SendError(_) => JobErrorOutcome::Fatal(e.to_string()),
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

    #[test]
    fn test_error_outcome_from_io_error() {
        let outcome: JobErrorOutcome =
            std::io::Error::new(std::io::ErrorKind::NotFound, "file not found").into();
        assert!(
            matches!(outcome, JobErrorOutcome::Retryable(msg) if msg.contains("file not found"))
        );
    }

    #[test]
    fn test_error_outcome_from_dyn_error() {
        let dyn_err: DynError =
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, "some error"));
        let outcome = JobErrorOutcome::from(dyn_err);
        assert!(matches!(outcome, JobErrorOutcome::Retryable(msg) if msg.contains("some error")));
    }

    #[test]
    fn test_error_outcome_display() {
        let retryable = JobErrorOutcome::Retryable("try again".into());
        assert_eq!(retryable.to_string(), "Retryable: try again");

        let fatal = JobErrorOutcome::Fatal("give up".into());
        assert_eq!(fatal.to_string(), "Fatal: give up");
    }

    #[test]
    fn test_job_error_from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "access denied");
        let job_err = JobError::from(io_err);
        assert!(matches!(job_err, JobError::IoError(_)));
        assert!(job_err.to_string().contains("access denied"));
    }

    #[test]
    fn test_tool_error_from_string() {
        let err = ToolError::from("custom message".to_string());
        assert!(matches!(err, ToolError::Message(_)));
        assert_eq!(err.to_string(), "custom message");
    }

    #[test]
    fn test_job_error_variants_display() {
        let io = JobError::IoError(std::io::Error::new(std::io::ErrorKind::Other, "io"));
        assert!(io.to_string().contains("io"));

        let wreq = JobError::OtherFatal("fatal".into());
        assert_eq!(wreq.to_string(), "fatal");
    }

    #[test]
    fn test_tool_error_exit_code_backend_connection() {
        let mongo_conn =
            ToolError::MongoConnectionError(bb8_mongodb_error::MongoDB(make_mongo_error()));
        assert_eq!(mongo_conn.exit_code(), 3);

        let bson = ToolError::BsonError(
            mongodb::bson::Document::from_reader(std::io::Cursor::new(b"\x00\x00\x00\x00"))
                .unwrap_err(),
        );
        assert_eq!(bson.exit_code(), 3);
    }
}
