use std::env;
use std::io::Write;
use std::path::PathBuf;

use ingest_core::OutputFormat;
use ingest_core::bootstrap;
use ingest_core::config::RunConfig;
use ingest_core::models::{self, Resource, load_config};
use ingest_core::{AppConfig, MongoService, TomlRawConfig, ToolError};

const TOML_TEST: &str = r#"
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
"#;

fn mongo_uri() -> String {
    env::var("MONGODB_URI").unwrap_or_else(|_| {
        "mongodb://root:example@localhost:27017/ingestion?authSource=admin".into()
    })
}

fn redis_uri() -> String {
    env::var("REDIS_URI").unwrap_or_else(|_| "redis://localhost:6379".into())
}

fn setup_toml() -> TomlRawConfig {
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

fn build_app_config(yaml_path: PathBuf, no_follow: bool, dry_run: bool) -> (AppConfig, PathBuf) {
    let toml = setup_toml();
    let yaml_config = load_config(&yaml_path).expect("load test YAML");
    let args = RunConfig {
        yaml_path: yaml_path.clone(),
        dry_run,
        priority: None,
        workers: None,
        follow: !no_follow,
        no_follow,
        output: OutputFormat::Json,
    };
    let cfg = AppConfig::from_sources(&yaml_config, toml, args, redis_uri(), mongo_uri());
    (cfg, yaml_path)
}

fn build_app_config_full(yaml_path: PathBuf, args: RunConfig) -> (AppConfig, PathBuf) {
    let toml = setup_toml();
    let yaml_config = load_config(&yaml_path).expect("load test YAML");
    let cfg = AppConfig::from_sources(&yaml_config, toml, args, redis_uri(), mongo_uri());
    (cfg, yaml_path)
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

    let (config, yaml_path) = build_app_config(yaml_path, true, false);
    let yaml_config = load_config(&yaml_path).expect("load test YAML");
    let resources = yaml_config.resources;
    bootstrap::enqueue(&config, &resources)
        .await
        .expect("enqueue should succeed");

    let after = count_jobs(&mongo).await;
    assert_eq!(after, before + 2, "expected 2 new file jobs in MongoDB");
}

#[tokio::test]
async fn run_empty_yaml_is_accepted() {
    let (_dir, yaml_path) = write_test_yaml("");

    let (config, yaml_path) = build_app_config(yaml_path, true, false);
    let yaml_config = load_config(&yaml_path).expect("load test YAML");
    bootstrap::enqueue(&config, &yaml_config.resources)
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

    let (config, yaml_path) = build_app_config(yaml_path, true, false);
    let yaml_config = load_config(&yaml_path).expect("load test YAML");
    let err = bootstrap::enqueue(&config, &yaml_config.resources)
        .await
        .unwrap_err();

    assert!(matches!(err, ToolError::ValidationError(_)));
}

#[tokio::test]
async fn run_dry_run_local_file() {
    let (_dir, yaml_path) = write_test_yaml(
        r#"  - url: file:///etc/hosts
    name: hosts
"#,
    );

    let args = RunConfig {
        yaml_path: yaml_path.clone(),
        dry_run: true,
        priority: None,
        workers: None,
        follow: false,
        no_follow: false,
        output: OutputFormat::Json,
    };
    let (config, yaml_path) = build_app_config_full(yaml_path, args);
    let yaml_config = load_config(&yaml_path).expect("load test YAML");
    bootstrap::enqueue(&config, &yaml_config.resources)
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

    let args = RunConfig {
        yaml_path: yaml_path.clone(),
        dry_run: true,
        priority: None,
        workers: None,
        follow: false,
        no_follow: false,
        output: OutputFormat::Json,
    };
    let (config, yaml_path) = build_app_config_full(yaml_path, args);
    let yaml_config = load_config(&yaml_path).expect("load test YAML");
    let err = bootstrap::enqueue(&config, &yaml_config.resources)
        .await
        .unwrap_err();
    assert!(matches!(err, ToolError::ValidationError(_)));
}

#[tokio::test]
async fn run_dry_run_invalid_scheme_fails() {
    let (_dir, yaml_path) = write_test_yaml(
        r#"  - url: "gopher://example.com/resource"
    name: badscheme
"#,
    );

    let args = RunConfig {
        yaml_path: yaml_path.clone(),
        dry_run: true,
        priority: None,
        workers: None,
        follow: false,
        no_follow: false,
        output: OutputFormat::Json,
    };
    let (config, yaml_path) = build_app_config_full(yaml_path, args);
    let yaml_config = load_config(&yaml_path).expect("load test YAML");
    let err = bootstrap::enqueue(&config, &yaml_config.resources)
        .await
        .unwrap_err();
    assert!(matches!(err, ToolError::ValidationError(_)));
}
