use media_resources_ingestion::models::*;
use media_resources_ingestion::storage::Provider;
use url::Url;

#[test]
fn test_metadata_new_defaults() {
    let url = Url::parse("https://example.com/file.png").unwrap();
    let meta = Metadata::new(
        "abc123".into(),
        url.clone(),
        Provider::Local,
        "/tmp/file.png".into(),
        1024,
        None,
        "image/png".into(),
    );
    assert_eq!(meta.file_hash, "abc123");
    assert_eq!(meta.original_url, url);
    assert_eq!(meta.storage_provider, Provider::Local);
    assert_eq!(meta.storage_path, "/tmp/file.png");
    assert_eq!(meta.original_file_size, 1024);
    assert!(meta.compressed_file_size.is_none());
    assert!(meta.compression_ratio.is_none());
    assert_eq!(meta.mime_type, "image/png");
    assert!(meta.chunk_manifest.is_none());
    assert_eq!(meta.duplicate_reference_count, 0);
    assert!(meta.update_date.is_none());
}

#[test]
fn test_metadata_new_with_compression() {
    let url = Url::parse("https://example.com/video.mp4").unwrap();
    let meta = Metadata::new(
        "def456".into(),
        url,
        Provider::S3,
        "bucket/key".into(),
        5000,
        Some(2000),
        "video/mp4".into(),
    );
    assert_eq!(meta.compressed_file_size, Some(2000));
    assert!(meta.compression_ratio.is_some());
    assert!((meta.compression_ratio.unwrap() - 0.4).abs() < f32::EPSILON);
}

#[test]
fn test_metadata_serde_roundtrip() {
    let url = Url::parse("https://example.com/image.webp").unwrap();
    let meta = Metadata::new(
        "hash789".into(),
        url,
        Provider::Gdrive,
        "drive:/path".into(),
        100,
        Some(50),
        "image/webp".into(),
    );
    let json = serde_json::to_string(&meta).unwrap();
    let deser: Metadata = serde_json::from_str(&json).unwrap();
    assert_eq!(deser.file_hash, meta.file_hash);
    assert_eq!(deser.original_url, meta.original_url);
    assert_eq!(deser.mime_type, meta.mime_type);
    assert_eq!(deser.original_file_size, meta.original_file_size);
}

#[test]
fn test_manifest_serde() {
    let manifest = Manifest {
        chunks: vec![ChunkRef {
            hash: "c1".into(),
            size_original: 1000,
            size_compressed: Some(500),
            storage_path: "/chunks/c1".into(),
            offset_start: 0,
            offset_end: 999,
        }],
        compression: Some("image/webp".into()),
        original_size: 1000,
        compressed_size: 500,
    };
    let json = serde_json::to_string(&manifest).unwrap();
    let deser: Manifest = serde_json::from_str(&json).unwrap();
    assert_eq!(deser.chunks.len(), 1);
    assert_eq!(deser.chunks[0].hash, "c1");
}

#[test]
fn test_resource_default_uuid() {
    let resource: Resource = serde_yaml::from_str(
        r#"
        url: "https://example.com/file.txt"
    "#,
    )
    .unwrap();
    assert!(!resource.id.is_empty());
    assert_eq!(resource.url.to_string(), "https://example.com/file.txt");
    assert!(resource.name.is_none());
    assert!(resource.priority.is_none());
    assert!(resource.dest.is_none());
    assert!(resource.config.is_none());
}

#[test]
fn test_resource_with_all_fields() {
    let yaml = r#"
        url: "https://example.com/img.png"
        name: my_image
        priority: 5
        dest:
          provider: s3
          path: /bucket/images/
    "#;
    let resource: Resource = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(resource.name.unwrap(), "my_image");
    assert_eq!(resource.priority.unwrap(), 5);
    let dest = resource.dest.unwrap();
    assert_eq!(dest.provider.unwrap().to_string(), "s3");
    assert_eq!(dest.path.unwrap(), "/bucket/images/");
}

#[test]
fn test_ingestion_config_empty_resources() {
    let config: IngestionConfig = serde_yaml::from_str(
        r#"
        resources: []
    "#,
    )
    .unwrap();
    assert!(config.resources.is_empty());
}

#[test]
fn test_ingestion_config_minimal() {
    let config: IngestionConfig = serde_yaml::from_str(
        r#"
        resources:
          - url: "https://example.com/f.png"
    "#,
    )
    .unwrap();
    assert_eq!(config.resources.len(), 1);
    assert!(config.priority.is_none());
    assert!(config.chunk_size.is_none());
}

#[test]
fn test_ingestion_config_with_all_top_level() {
    let config: IngestionConfig = serde_yaml::from_str(
        r#"
        provider: s3
        path: /bucket
        priority: 10
        chunk_size: 256MB
        compression_override: webp
        resources:
          - url: "https://example.com/f.png"
    "#,
    )
    .unwrap();
    let dest = config.default_dest.unwrap();
    assert_eq!(dest.provider.unwrap().to_string(), "s3");
    assert_eq!(dest.path.unwrap(), "/bucket");
    assert_eq!(config.priority.unwrap(), 10);
    assert_eq!(config.chunk_size.unwrap(), "256MB");
}

#[test]
fn test_destination_default_serde() {
    let dest: Destination = serde_yaml::from_str(
        r#"
        path: /some/path
    "#,
    )
    .unwrap();
    assert!(dest.provider.is_none());
    assert_eq!(dest.path.unwrap(), "/some/path");
}

#[test]
fn test_compression_override_webp() {
    let override_val: CompressionOverride = serde_yaml::from_str("webp").unwrap();
    assert!(matches!(
        override_val,
        CompressionOverride::Image(ImageCompressionStrategy::Webp)
    ));
}

#[test]
fn test_compression_override_none() {
    let override_val: CompressionOverride = serde_yaml::from_str("none").unwrap();
    assert!(matches!(
        override_val,
        CompressionOverride::Generic(GenericCompressionStrategy::None)
    ));
}

#[test]
fn test_resource_level_config_serde() {
    let config: ResourceLevelConfig = serde_yaml::from_str(
        r#"
        quality: 85
        compression_override: avif
    "#,
    )
    .unwrap();
    assert_eq!(config.quality.unwrap(), 85);
    assert!(config.compression_override.is_some());
}

#[test]
fn test_headers_serde() {
    let headers: Headers = serde_yaml::from_str(
        r#"
        authorization: "Bearer token123"
        cookie: "session=abc"
    "#,
    )
    .unwrap();
    assert_eq!(headers.authorization.unwrap(), "Bearer token123");
    assert_eq!(headers.cookie.unwrap(), "session=abc");
}

#[test]
fn test_compression_override_all_image_variants() {
    let cases = [
        ("webp", ImageCompressionStrategy::Webp),
        ("avif", ImageCompressionStrategy::Avif),
        ("losslesswebp", ImageCompressionStrategy::LosslessWebp),
    ];
    for (yaml, expected) in &cases {
        let val: CompressionOverride = serde_yaml::from_str(yaml).unwrap();
        assert!(
            matches!(&val, CompressionOverride::Image(s) if s == expected),
            "failed for input {yaml:?}, got {val:?}"
        );
    }
}

#[test]
fn test_compression_override_all_video_variants() {
    let cases = [
        ("h265", VideoCompressionStrategy::H265),
        ("av1", VideoCompressionStrategy::Av1),
    ];
    for (yaml, expected) in &cases {
        let val: CompressionOverride = serde_yaml::from_str(yaml).unwrap();
        assert!(
            matches!(&val, CompressionOverride::Video(s) if s == expected),
            "failed for input {yaml:?}, got {val:?}"
        );
    }
}

#[test]
fn test_resource_level_config_with_all_overrides() {
    let overrides = [
        "webp",
        "avif",
        "losslesswebp",
        "originalformat",
        "h265",
        "av1",
        "zstd",
        "zip",
        "sevenz",
        "none",
    ];
    for co in &overrides {
        let yaml = format!(
            r#"
            quality: 90
            compression_override: {co}
        "#
        );
        let config: ResourceLevelConfig = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(config.quality, Some(90));
        assert!(config.compression_override.is_some(), "failed for {co}");
    }
}

#[test]
fn test_chunk_ref_defaults() {
    let cr: ChunkRef = serde_json::from_str(r#"
        {"hash": "c1", "size_original": 100, "size_compressed": null, "storage_path": "/p", "offset_start": 0, "offset_end": 99}
    "#).unwrap();
    assert_eq!(cr.hash, "c1");
    assert!(cr.size_compressed.is_none());
}
