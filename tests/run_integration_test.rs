use std::env;
use std::io::Write;
use std::path::PathBuf;

use media_resources_ingestion::bootstrap;
use media_resources_ingestion::cli::{OutputFormat, RunArgs, load_config};
use media_resources_ingestion::error::ToolError;
use media_resources_ingestion::models::MainConfig;
use media_resources_ingestion::services::mongo::MongoService;
use media_resources_ingestion::settings::{TomlConfig, merge_configs_yaml};

const TOML_TEST: &str = r#"
[cli]
log_format = "Pretty"
no_color = true
[cli.custom_flags]
dry_run = false
follow = true
output = "table"
[scheduler]
file_workers = 2
chunk_workers = 4
max_pending_jobs = 100
max_per_host = 2
[compression]
threshold_mb = 512
quality = 95
[storage]
default_provider = "local"
default_path = "/tmp/ingest-inttest"
chunk_size = "128MB"
temp_dir = "/tmp/ingest-inttest"
[retry]
attempt_1_secs = 1
attempt_2_secs = 1
attempt_3_secs = 1
"#;

fn mongo_uri() -> String {
    env::var("MONGODB_URI").unwrap_or_else(|_| {
        "mongodb://root:example@localhost:27017/ingestion?authSource=admin".into()
    })
}

fn redis_uri() -> String {
    env::var("REDIS_URI").unwrap_or_else(|_| "redis://localhost:6379".into())
}

fn setup_toml() -> TomlConfig {
    toml::from_str(TOML_TEST).expect("invalid test TOML")
}

fn temp_dir() -> PathBuf {
    std::env::temp_dir().join(uuid::Uuid::new_v4().to_string())
}

fn write_test_yaml(resources: &str) -> (PathBuf, PathBuf) {
    let dir = temp_dir();
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let path = dir.join("test-ingest.yaml");
    let content = format!("resources:\n{}", resources);
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(content.as_bytes()).unwrap();
    f.flush().unwrap();
    (dir, path)
}

fn build_run_args(yaml_path: PathBuf, no_follow: bool) -> RunArgs {
    RunArgs {
        yaml_path,
        dry_run: false,
        priority: None,
        workers: None,
        follow: !no_follow,
        no_follow,
        output: OutputFormat::Json,
    }
}

fn build_config(yaml_path: PathBuf) -> MainConfig {
    let toml = setup_toml();
    let yaml_config = load_config(&yaml_path).expect("load test YAML");
    let merged = merge_configs_yaml(&yaml_config, toml).expect("merge configs");
    MainConfig {
        toml_config: merged,
        yaml_config,
        yaml_path,
        redis_uri: redis_uri(),
        mongo_uri: mongo_uri(),
    }
}

async fn count_jobs(mongo: &MongoService) -> usize {
    mongo.list_jobs(None, 10000).await.unwrap_or_default().len()
}

#[tokio::test]
async fn run_no_follow_creates_batch_and_jobs() {
    let (_dir, yaml_path) = write_test_yaml(
        r#"  - url: https://example.com/img1.png
    name: img1
    dest:
      path: /tmp/ingest-inttest
  - url: https://example.com/img2.png
    name: img2
    dest:
      path: /tmp/ingest-inttest
"#,
    );

    let mongo = MongoService::new(&mongo_uri())
        .await
        .expect("connect to MongoDB");
    let before = count_jobs(&mongo).await;

    let config = build_config(yaml_path.clone());
    let args = build_run_args(yaml_path, true);
    bootstrap::run(config, args)
        .await
        .expect("run --no-follow should succeed");

    let after = count_jobs(&mongo).await;
    assert_eq!(after, before + 2, "expected 2 new file jobs in MongoDB");
}

#[tokio::test]
async fn run_empty_yaml_is_accepted() {
    let (_dir, yaml_path) = write_test_yaml("");

    let config = build_config(yaml_path.clone());
    let args = build_run_args(yaml_path, true);
    bootstrap::run(config, args)
        .await
        .expect("empty yaml should succeed");
}

#[tokio::test]
async fn run_duplicate_urls_rejected() {
    let (_dir, yaml_path) = write_test_yaml(
        r#"  - url: https://example.com/dup.png
    name: dup1
    dest:
      path: /tmp/ingest-inttest
  - url: https://example.com/dup.png
    name: dup2
    dest:
      path: /tmp/ingest-inttest
"#,
    );

    let config = build_config(yaml_path.clone());
    let args = build_run_args(yaml_path, true);
    let err = bootstrap::run(config, args).await.unwrap_err();

    assert!(matches!(err, ToolError::ValidationError(_)));
}

#[tokio::test]
async fn run_dry_run_local_file() {
    let (_dir, yaml_path) = write_test_yaml(
        r#"  - url: file:///etc/hosts
    name: hosts
"#,
    );

    let config = build_config(yaml_path.clone());
    let args = RunArgs {
        yaml_path,
        dry_run: true,
        priority: None,
        workers: None,
        follow: false,
        no_follow: false,
        output: OutputFormat::Json,
    };
    bootstrap::run(config, args)
        .await
        .expect("dry-run with existing file should succeed");
}

#[tokio::test]
async fn run_dry_run_missing_file_fails() {
    let (_dir, yaml_path) = write_test_yaml(
        r#"  - url: file:///tmp/nonexistent-test-file-ingest-12345
    name: missing
"#,
    );

    let config = build_config(yaml_path.clone());
    let args = RunArgs {
        yaml_path,
        dry_run: true,
        priority: None,
        workers: None,
        follow: false,
        no_follow: false,
        output: OutputFormat::Json,
    };
    let err = bootstrap::run(config, args).await.unwrap_err();
    assert!(matches!(err, ToolError::ValidationError(_)));
}

#[tokio::test]
async fn run_dry_run_invalid_scheme_fails() {
    let (_dir, yaml_path) = write_test_yaml(
        r#"  - url: "gopher://example.com/resource"
    name: badscheme
"#,
    );

    let config = build_config(yaml_path.clone());
    let args = RunArgs {
        yaml_path,
        dry_run: true,
        priority: None,
        workers: None,
        follow: false,
        no_follow: false,
        output: OutputFormat::Json,
    };
    let err = bootstrap::run(config, args).await.unwrap_err();
    assert!(matches!(err, ToolError::ValidationError(_)));
}
