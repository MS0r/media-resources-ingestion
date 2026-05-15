use media_resources_ingestion::error::*;

fn make_toml_error() -> toml::de::Error {
    toml::from_str::<()>("invalid = [").unwrap_err()
}

fn make_mongo_error() -> mongodb::error::Error {
    mongodb::error::Error::from(std::io::Error::new(std::io::ErrorKind::Other, "no server"))
}

fn make_image_error() -> image::ImageError {
    image::ImageError::Unsupported(
        image::error::UnsupportedError::from_format_and_kind(
            image::error::ImageFormatHint::Name("unknown".into()),
            image::error::UnsupportedErrorKind::Format(
                image::error::ImageFormatHint::Name("unknown".into()),
            ),
        ),
    )
}

#[test]
fn test_exit_code_config() {
    assert_eq!(ToolError::ConfigError("bad".into()).exit_code(), 2);
    assert_eq!(ToolError::ValidationError("bad".into()).exit_code(), 2);
    assert_eq!(ToolError::ConfigParseError(make_toml_error()).exit_code(), 2);
}

#[test]
fn test_exit_code_auth() {
    assert_eq!(ToolError::AuthError("denied".into()).exit_code(), 4);
}

#[test]
fn test_exit_code_backend() {
    assert_eq!(ToolError::RedisError(redis::RedisError::from(std::io::Error::new(std::io::ErrorKind::Other, "no server"))).exit_code(), 3);
    assert_eq!(ToolError::MongoError(make_mongo_error()).exit_code(), 3);
}

#[test]
fn test_exit_code_job_failure() {
    assert_eq!(ToolError::JobExecutionError("fail".into()).exit_code(), 1);
    assert_eq!(ToolError::IoError(std::io::Error::new(std::io::ErrorKind::Other, "fail")).exit_code(), 1);
    assert_eq!(ToolError::Message("msg".into()).exit_code(), 1);
    assert_eq!(ToolError::JsonError(serde_json::from_str::<()>("").unwrap_err()).exit_code(), 1);
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
