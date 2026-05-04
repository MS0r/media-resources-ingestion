use media_resources_ingestion::settings::TomlConfig;
use media_resources_ingestion::models::IngestionConfig;
use media_resources_ingestion::cli::LogFormat;
use media_resources_ingestion::settings::merge_configs_yaml;

const TOML_FULL: &str = r#"
[cli]
log_format = "Pretty"
no_color = false

[scheduler]
file_workers = 5
chunk_workers = 20
max_pending_jobs = 10000
max_per_host = 2

[compression]
threshold_mb = 512
quality = 95

[storage]
default_provider = "local"
default_path = "~/downloads"
chunk_size = "128MB"

[retry]
attempt_1_secs = 5
attempt_2_secs = 30
attempt_3_secs = 120
"#;

const TOML_MINIMAL: &str = r#"
[cli]
log_format = "Json"
no_color = true

[scheduler]
file_workers = 1
chunk_workers = 1
max_pending_jobs = 0
max_per_host = 1

[compression]
threshold_mb = 0
quality = 0

[storage]
default_provider = "s3"
default_path = "~"
chunk_size = "64MB"

[retry]
attempt_1_secs = 1
attempt_2_secs = 1
attempt_3_secs = 1
"#;

const TOML_LARGE: &str = r#"
[cli]
log_format = "Pretty"
no_color = false

[scheduler]
file_workers = 100
chunk_workers = 200
max_pending_jobs = 1000000
max_per_host = 10

[compression]
threshold_mb = 1024
quality = 100

[storage]
default_provider = "s3"
default_path = "/mnt/storage"
chunk_size = "512MB"

[retry]
attempt_1_secs = 60
attempt_2_secs = 300
attempt_3_secs = 600
"#;

const YAML_FULL: &str = r#"
provider: local
path: ~/images
priority: 0
chunk_size: 128MB
compression_override: webp
headers:
  authorization: Bearer Token
  cookie: session=abc
resources:
  - url: https://example.com/image.webp
    name: image
    priority: 10
    dest:
      provider: local
      path: ~/downloads
    config:
      force_compress: true
      compression_override: webp
      quality: 95
"#;

const YAML_MINIMAL: &str = r#"
resources:
  - url: https://example.com/file.txt
"#;

const YAML_MULTIPLE_RESOURCES: &str = r#"
provider: s3
path: /mnt/bucket
priority: 5
resources:
  - url: https://example.com/image1.png
    name: first_image
    priority: 10
  - url: https://example.com/image2.jpg
    name: second_image
  - url: https://example.com/image3.gif
"#;

const YAML_DEST: &str = r#"
resources:
  - url: https://example.com/test.png
    dest:
      path: /custom/path
"#;

const YAML_HEADERS: &str = r#"
headers:
  authorization: Bearer mytoken123
resources:
  - url: https://api.example.com/data.json
"#;

const YAML_RESOURCE_CONFIG: &str = r#"
resources:
  - url: https://example.com/image.png
    config:
      force_compress: false
      quality: 80
"#;

mod toml_tests {
    use super::*;

    #[test]
    fn test_toml_config_full_deserialization() {
        let config: TomlConfig = toml::from_str(TOML_FULL).expect("Failed to parse TOML");

        assert_eq!(config.cli.log_format, LogFormat::Pretty);
        assert_eq!(config.cli.no_color, false);
        assert_eq!(config.scheduler.file_workers, 5);
        assert_eq!(config.scheduler.chunk_workers, 20);
        assert_eq!(config.scheduler.max_pending_jobs, 10000);
        assert_eq!(config.scheduler.max_per_host, 2);
        assert_eq!(config.compression.threshold_mb, 512);
        assert_eq!(config.compression.quality, 95);
        assert_eq!(config.storage.default_provider, "local");
        assert_eq!(config.storage.default_path, "~/downloads");
        assert_eq!(config.storage.chunk_size, "128MB");
        assert_eq!(config.retry.attempt_1_secs, 5);
        assert_eq!(config.retry.attempt_2_secs, 30);
        assert_eq!(config.retry.attempt_3_secs, 120);
    }

    #[test]
    fn test_toml_config_minimal_values() {
        let config: TomlConfig = toml::from_str(TOML_MINIMAL).expect("Failed to parse TOML");

        assert_eq!(config.cli.log_format, LogFormat::Json);
        assert_eq!(config.cli.no_color, true);
        assert_eq!(config.scheduler.file_workers, 1);
        assert_eq!(config.compression.quality, 0);
    }

    #[test]
    fn test_toml_config_large_values() {
        let config: TomlConfig = toml::from_str(TOML_LARGE).expect("Failed to parse TOML");

        assert_eq!(config.scheduler.file_workers, 100);
        assert_eq!(config.scheduler.chunk_workers, 200);
        assert_eq!(config.scheduler.max_pending_jobs, 1_000_000);
        assert_eq!(config.compression.threshold_mb, 1024);
        assert_eq!(config.compression.quality, 100);
    }
}

mod yaml_tests {
    use super::*;

    #[test]
    fn test_yaml_config_full_deserialization() {
        let config: IngestionConfig = serde_yaml::from_str(YAML_FULL).expect("Failed to parse YAML");

        assert!(config.default_dest.is_some());
        let default_dest = config.default_dest.unwrap();
        assert!(default_dest.provider.is_some());
        assert_eq!(default_dest.provider.unwrap().to_string(), "local");
        assert_eq!(default_dest.path.unwrap(), "~/images");
        assert_eq!(config.priority, Some(0));
        assert_eq!(config.chunk_size, Some("128MB".to_string()));

        let headers = config.headers.expect("headers should be present");
        assert_eq!(headers.authorization, Some("Bearer Token".to_string()));
        assert_eq!(headers.cookie, Some("session=abc".to_string()));

        assert_eq!(config.resources.len(), 1);
        let resource = &config.resources[0];
        assert_eq!(resource.url.to_string(), "https://example.com/image.webp");
        assert_eq!(resource.name, Some("image".to_string()));
        assert_eq!(resource.priority, Some(10));

        let dest = resource.dest.as_ref().expect("dest should be present");
        assert_eq!(dest.provider.as_ref().expect("provider should be present").to_string(), "local");
        assert_eq!(dest.path.as_ref().expect("path should be present"), "~/downloads");

        let resource_config = resource.config.as_ref().expect("config should be present");
        assert_eq!(resource_config.quality, Some(95));
    }

    #[test]
    fn test_yaml_config_minimal() {
        let config: IngestionConfig = serde_yaml::from_str(YAML_MINIMAL).expect("Failed to parse YAML");

        assert!(config.default_dest.is_some());
        assert!(config.default_dest.as_ref().unwrap().provider.is_none());
        assert!(config.default_dest.as_ref().unwrap().path.is_none());
        assert!(config.priority.is_none());
        assert!(config.chunk_size.is_none());
        assert!(config.headers.is_none());

        assert_eq!(config.resources.len(), 1);
        let resource = &config.resources[0];
        assert_eq!(resource.url.to_string(), "https://example.com/file.txt");
        assert!(resource.name.is_none());
        assert!(resource.priority.is_none());
        assert!(resource.dest.is_none());
        assert!(resource.config.is_none());
    }

    #[test]
    fn test_yaml_config_multiple_resources() {
        let config: IngestionConfig = serde_yaml::from_str(YAML_MULTIPLE_RESOURCES).expect("Failed to parse YAML");

        assert_eq!(config.resources.len(), 3);

        assert_eq!(config.resources[0].name, Some("first_image".to_string()));
        assert_eq!(config.resources[0].priority, Some(10));

        assert_eq!(config.resources[1].name, Some("second_image".to_string()));
        assert!(config.resources[1].priority.is_none());

        assert_eq!(config.resources[2].url.to_string(), "https://example.com/image3.gif");
        assert!(config.resources[2].name.is_none());
    }

    #[test]
    fn test_yaml_config_destination_defaults() {
        let config: IngestionConfig = serde_yaml::from_str(YAML_DEST).expect("Failed to parse YAML");

        let dest = config.resources[0].dest.as_ref().expect("dest should be present");
        assert!(dest.provider.is_none());
        assert_eq!(dest.path, Some("/custom/path".to_string()));
    }

    #[test]
    fn test_yaml_config_headers_only() {
        let config: IngestionConfig = serde_yaml::from_str(YAML_HEADERS).expect("Failed to parse YAML");

        let headers = config.headers.expect("headers should be present");
        assert_eq!(headers.authorization, Some("Bearer mytoken123".to_string()));
        assert!(headers.cookie.is_none());
    }

    #[test]
    fn test_yaml_config_resource_level_config() {
        let config: IngestionConfig = serde_yaml::from_str(YAML_RESOURCE_CONFIG).expect("Failed to parse YAML");

        let resource_config = config.resources[0].config.as_ref().expect("config should be present");
        assert_eq!(resource_config.quality, Some(80));
        assert!(resource_config.compression_override.is_none());
    }
}

mod merge_tests {
    use media_resources_ingestion::error::BoxedError;

    use super::*;

    const TOML_DEFAULTS: &str = r#"
[cli]
log_format = "Pretty"
no_color = false

[scheduler]
file_workers = 5
chunk_workers = 20
max_pending_jobs = 10000
max_per_host = 2

[compression]
threshold_mb = 512
quality = 95

[storage]
default_provider = "local"
default_path = "~/downloads"
chunk_size = "128MB"

[retry]
attempt_1_secs = 5
attempt_2_secs = 30
attempt_3_secs = 120
"#;

    const YAML_MINIMAL_NO_PROVIDER: &str = r#"
resources:
  - url: https://example.com/file.txt
"#;

    const YAML_WITH_PROVIDER: &str = r#"
provider: s3
path: /custom/path
resources:
  - url: https://example.com/file.txt
"#;

    const YAML_WITH_CHUNK_SIZE: &str = r#"
chunk_size: 256MB
resources:
  - url: https://example.com/file.txt
"#;

    #[test]
    fn test_merge_provider_from_toml() -> Result<(), BoxedError> {
        let toml: TomlConfig = toml::from_str(TOML_DEFAULTS).expect("Failed to parse TOML");
        let yaml: IngestionConfig = serde_yaml::from_str(YAML_MINIMAL_NO_PROVIDER).expect("Failed to parse YAML");

        let merged = merge_configs_yaml(&yaml, toml)?;

        assert_eq!(merged.storage.default_provider, "local");
        Ok(())
    }

    #[test]
    fn test_merge_path_from_toml() -> Result<(), BoxedError>  {
        let toml: TomlConfig = toml::from_str(TOML_DEFAULTS).expect("Failed to parse TOML");
        let yaml: IngestionConfig = serde_yaml::from_str(YAML_MINIMAL_NO_PROVIDER).expect("Failed to parse YAML");

        let merged = merge_configs_yaml(&yaml, toml)?;

        assert_eq!(merged.storage.default_path, "~/downloads");
        Ok(())
    }

    #[test]
    fn test_merge_chunk_size_from_toml() -> Result<(), BoxedError>  {
        let toml: TomlConfig = toml::from_str(TOML_DEFAULTS).expect("Failed to parse TOML");
        let yaml: IngestionConfig = serde_yaml::from_str(YAML_MINIMAL_NO_PROVIDER).expect("Failed to parse YAML");

        let merged = merge_configs_yaml(&yaml, toml)?;

        assert_eq!(merged.storage.chunk_size, "128MB");
        Ok(())
    }

    #[test]
    fn test_merge_yaml_overrides_toml() -> Result<(), BoxedError>  {
        let toml: TomlConfig = toml::from_str(TOML_DEFAULTS).expect("Failed to parse TOML");
        let yaml: IngestionConfig = serde_yaml::from_str(YAML_WITH_PROVIDER).expect("Failed to parse YAML");

        let merged = merge_configs_yaml(&yaml, toml)?;

        assert_eq!(merged.storage.default_provider, "s3");
        assert_eq!(merged.storage.default_path, "/custom/path");
        Ok(())
    }

    #[test]
    fn test_merge_yaml_chunk_size_overrides_toml() -> Result<(), BoxedError>  {
        let toml: TomlConfig = toml::from_str(TOML_DEFAULTS).expect("Failed to parse TOML");
        let yaml: IngestionConfig = serde_yaml::from_str(YAML_WITH_CHUNK_SIZE).expect("Failed to parse YAML");

        let merged = merge_configs_yaml(&yaml, toml)?;

        assert_eq!(merged.storage.chunk_size, "256MB");
        Ok(())
    }
}