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
    #[error("Source auth provider '{0}' not registered in auth registry")]
    Unregistered(String),
    #[error("Token refresh failed for '{provider}': {error}")]
    TokenRefresh { provider: String, error: String },
    #[error("Failed to generate S3 presigned URL: {0}")]
    S3Presign(String),
}

/// Unified error type — replaces `ToolError` + `JobError` + `JobErrorOutcome`.
#[derive(Error, Debug)]
pub enum AppError {
    // ── Infrastructure / config (exit 2 / 3 / 4) ────────────────────────
    #[error("Config error: {0}")]
    Config(String),
    #[error("YAML parse error: {0}")]
    Yaml(String),
    #[error("TOML parse error: {0}")]
    Toml(String),
    #[error("MongoDB error: {0}")]
    Mongo(#[from] mongodb_error),
    #[error("MongoDB pool error: {0}")]
    MongoPool(#[from] bb8_error<bb8_mongodb_error>),
    #[error("Redis error: {0}")]
    Redis(#[from] redis_error),
    #[error("I/O error: {0}")]
    Io(#[from] io_error),
    #[error("HTTP error: {0}")]
    Http(#[from] wreq::Error),
    #[error("JSON error: {0}")]
    Json(#[from] json_error),
    #[error("URL parse error: {0}")]
    Url(#[from] url_parse_error),
    #[error("Env var error: {0}")]
    Env(#[from] env_error),
    #[error("Server error: {0}")]
    Grpc(#[from] tonic::transport::Error),

    // ── Auth ─────────────────────────────────────────────────────────────
    #[error("Auth error: {0}")]
    Auth(String),
    #[error("Auth not available: {0}")]
    AuthUnavailable(String),
    #[error("Auth token refresh failed for '{provider}': {error}")]
    AuthRefresh { provider: String, error: String },
    #[error("S3 presign failed: {0}")]
    S3Presign(String),

    // ── Job-side errors (carry to JobOutcome) ────────────────────────────
    #[error("{0}")]
    JobRetryable(String),
    #[error("{0}")]
    JobFatal(String),

    // ── Misc ─────────────────────────────────────────────────────────────
    #[error("{0}")]
    Message(String),
    #[error("Interrupted")]
    Interrupted,
    #[error("Not implemented: {0}")]
    NotImplemented(&'static str),
}

impl From<String> for AppError {
    fn from(s: String) -> Self {
        AppError::Message(s)
    }
}

impl AppError {
    pub fn exit_code(&self) -> i32 {
        match self {
            AppError::Config(_) | AppError::Yaml(_) | AppError::Toml(_) => 2,
            AppError::Mongo(_) | AppError::MongoPool(_) | AppError::Redis(_) => 3,
            AppError::Auth(_)
            | AppError::AuthUnavailable(_)
            | AppError::AuthRefresh { .. }
            | AppError::S3Presign(_) => 4,
            AppError::Interrupted => 130,
            _ => 1,
        }
    }

    /// Returns `true` if this error is retryable (transient infrastructure failure).
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            AppError::Mongo(_)
                | AppError::MongoPool(_)
                | AppError::Redis(_)
                | AppError::Io(_)
                | AppError::Http(_)
                | AppError::Json(_)
                | AppError::JobRetryable(_)
        )
    }
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

// ── From<ToolError> for AppError (backward compatibility) ─────────────────

impl From<ToolError> for AppError {
    fn from(e: ToolError) -> Self {
        match e {
            ToolError::RedisError(e) => AppError::Redis(e),
            ToolError::MongoError(e) => AppError::Mongo(e),
            ToolError::MongoPoolError(e) => AppError::MongoPool(e),
            ToolError::MongoConnectionError(e) => AppError::Message(e.to_string()),
            ToolError::BsonError(e) => AppError::Message(e.to_string()),
            ToolError::ConfigParseError(e) => AppError::Toml(e.to_string()),
            ToolError::YamlError(e) => AppError::Yaml(e.to_string()),
            ToolError::JsonError(e) => AppError::Json(e),
            ToolError::IoError(e) => AppError::Io(e),
            ToolError::EnvError(e) => AppError::Env(e),
            ToolError::WreqError(e) => AppError::Http(e),
            ToolError::UrlParseError(e) => AppError::Url(e),
            ToolError::Message(s) => AppError::Message(s),
            ToolError::ConfigError(s) => AppError::Config(s),
            ToolError::ValidationError(s) => AppError::Message(s),
            ToolError::AuthError(s) => AppError::Auth(s),
            ToolError::AuthResolution(e) => match e {
                AuthResolutionError::Unregistered(s) => AppError::AuthUnavailable(s),
                AuthResolutionError::TokenRefresh { provider, error } => {
                    AppError::AuthRefresh { provider, error }
                }
                AuthResolutionError::S3Presign(s) => AppError::S3Presign(s),
            },
            ToolError::JobExecutionError(s) => AppError::JobRetryable(s),
            ToolError::SemaphoreError(e) => AppError::Message(e.to_string()),
            ToolError::ServerError(e) => AppError::Grpc(e),
            ToolError::Interrupted => AppError::Interrupted,
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
            | ToolError::AuthError(_)
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

// ── From<AppError> for JobOutcome ──────────────────────────────────────────

impl From<AppError> for crate::job::JobOutcome {
    fn from(e: AppError) -> Self {
        if e.is_retryable() {
            crate::job::JobOutcome::Retry {
                reason: e.to_string(),
                backoff_secs: 30,
            }
        } else {
            crate::job::JobOutcome::Fail {
                reason: e.to_string(),
            }
        }
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
    fn test_app_error_exit_code_config() {
        assert_eq!(AppError::Config("bad".into()).exit_code(), 2);
        assert_eq!(AppError::Yaml("bad".into()).exit_code(), 2);
        assert_eq!(AppError::Toml("bad".into()).exit_code(), 2);
    }

    #[test]
    fn test_app_error_exit_code_auth() {
        assert_eq!(AppError::Auth("denied".into()).exit_code(), 4);
        assert_eq!(AppError::AuthUnavailable("gdrive".into()).exit_code(), 4);
        assert_eq!(AppError::S3Presign("failed".into()).exit_code(), 4);
    }

    #[test]
    fn test_app_error_exit_code_backend() {
        assert_eq!(
            AppError::Redis(redis::RedisError::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                "conn"
            )))
            .exit_code(),
            3
        );
        assert_eq!(AppError::Mongo(make_mongo_error()).exit_code(), 3);
    }

    #[test]
    fn test_app_error_exit_code_job_failure() {
        assert_eq!(AppError::Message("msg".into()).exit_code(), 1);
        assert_eq!(
            AppError::Json(serde_json::from_str::<()>("").unwrap_err()).exit_code(),
            1
        );
    }

    #[test]
    fn test_app_error_exit_code_interrupted() {
        assert_eq!(AppError::Interrupted.exit_code(), 130);
    }

    #[test]
    fn test_app_error_is_retryable() {
        assert!(AppError::Mongo(make_mongo_error()).is_retryable());
        assert!(
            AppError::Redis(redis::RedisError::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                "conn"
            )))
            .is_retryable()
        );
        assert!(
            AppError::Io(std::io::Error::new(std::io::ErrorKind::Other, "retry")).is_retryable()
        );
        assert!(AppError::JobRetryable("retry".into()).is_retryable());

        assert!(!AppError::Config("fatal".into()).is_retryable());
        assert!(!AppError::Auth("denied".into()).is_retryable());
        assert!(!AppError::JobFatal("fatal".into()).is_retryable());
    }

    #[test]
    fn test_app_error_from_string() {
        let err = AppError::from("custom message".to_string());
        assert!(matches!(err, AppError::Message(_)));
        assert_eq!(err.to_string(), "custom message");
    }

    #[test]
    fn test_app_error_from_tool_error() {
        let app_err = AppError::from(ToolError::ConfigError("bad".into()));
        assert!(matches!(app_err, AppError::Config(_)));

        let app_err = AppError::from(ToolError::RedisError(redis::RedisError::from(
            std::io::Error::new(std::io::ErrorKind::Other, "conn"),
        )));
        assert!(matches!(app_err, AppError::Redis(_)));
    }

    #[test]
    fn test_job_outcome_from_app_error() {
        let outcome: crate::job::JobOutcome =
            AppError::Io(std::io::Error::new(std::io::ErrorKind::Other, "retry")).into();
        assert!(matches!(outcome, crate::job::JobOutcome::Retry { .. }));

        let outcome: crate::job::JobOutcome = AppError::Config("fatal".into()).into();
        assert!(matches!(outcome, crate::job::JobOutcome::Fail { .. }));
    }

    // ── Legacy tests (kept for backward compatibility) ───────────────────

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
