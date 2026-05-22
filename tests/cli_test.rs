use clap::Parser;
use media_resources_ingestion::cli::*;

#[test]
fn test_cli_run_basic() {
    let cli = Cli::try_parse_from(["ingest", "run", "test.yaml"]).unwrap();
    assert!(
        matches!(cli.command, Commands::Run(ref args) if args.yaml_path.to_string_lossy() == "test.yaml")
    );
}

#[test]
fn test_cli_run_all_flags() {
    let cli = Cli::try_parse_from([
        "ingest",
        "run",
        "test.yaml",
        "--dry-run",
        "--priority",
        "42",
        "--workers",
        "10",
        "--no-follow",
        "--output",
        "json",
    ])
    .unwrap();
    match cli.command {
        Commands::Run(args) => {
            assert!(args.dry_run);
            assert_eq!(args.priority, Some(42));
            assert_eq!(args.workers, Some(10));
            assert!(args.no_follow);
            assert!(matches!(args.output, OutputFormat::Json));
        }
        _ => panic!("expected Run command"),
    }
}

#[test]
fn test_cli_run_defaults() {
    let cli = Cli::try_parse_from(["ingest", "run", "test.yaml"]).unwrap();
    match cli.command {
        Commands::Run(args) => {
            assert!(!args.dry_run);
            assert_eq!(args.priority, None);
            assert_eq!(args.workers, None);
            assert!(args.follow);
            assert!(!args.no_follow);
            assert!(matches!(args.output, OutputFormat::Table));
        }
        _ => panic!("expected Run command"),
    }
}

#[test]
fn test_cli_run_follow_overrides_no_follow() {
    let cli =
        Cli::try_parse_from(["ingest", "run", "test.yaml", "--no-follow", "--follow"]).unwrap();
    match cli.command {
        Commands::Run(args) => {
            assert!(args.follow);
            assert!(!args.no_follow);
        }
        _ => panic!("expected Run command"),
    }
}

#[test]
fn test_cli_run_no_follow_overrides_follow() {
    let cli =
        Cli::try_parse_from(["ingest", "run", "test.yaml", "--follow", "--no-follow"]).unwrap();
    match cli.command {
        Commands::Run(args) => {
            assert!(!args.follow);
            assert!(args.no_follow);
        }
        _ => panic!("expected Run command"),
    }
}

#[test]
fn test_cli_status_batch() {
    let cli = Cli::try_parse_from(["ingest", "status", "batch", "b_abc123"]).unwrap();
    match cli.command {
        Commands::Status { scope } => match scope {
            StatusScope::Batch { batch_id } => assert_eq!(batch_id, "b_abc123"),
            _ => panic!("expected Batch scope"),
        },
        _ => panic!("expected Status command"),
    }
}

#[test]
fn test_cli_status_job() {
    let cli = Cli::try_parse_from(["ingest", "status", "job", "j_xyz"]).unwrap();
    match cli.command {
        Commands::Status { scope } => match scope {
            StatusScope::Job { job_id } => assert_eq!(job_id, "j_xyz"),
            _ => panic!("expected Job scope"),
        },
        _ => panic!("expected Status command"),
    }
}

#[test]
fn test_cli_status_jobs_defaults() {
    let cli = Cli::try_parse_from(["ingest", "status", "jobs"]).unwrap();
    match cli.command {
        Commands::Status { scope } => match scope {
            StatusScope::Jobs(args) => {
                assert!(args.filter.is_none());
                assert_eq!(args.limit, 50);
                assert!(matches!(args.output, OutputFormat::Table));
            }
            _ => panic!("expected Jobs scope"),
        },
        _ => panic!("expected Status command"),
    }
}

#[test]
fn test_cli_status_jobs_all_flags() {
    let cli = Cli::try_parse_from([
        "ingest", "status", "jobs", "--filter", "failed", "--limit", "100", "--output", "json",
    ])
    .unwrap();
    match cli.command {
        Commands::Status { scope } => match scope {
            StatusScope::Jobs(args) => {
                assert_eq!(args.filter, Some(JobStatus::Failed));
                assert_eq!(args.limit, 100);
                assert!(matches!(args.output, OutputFormat::Json));
            }
            _ => panic!("expected Jobs scope"),
        },
        _ => panic!("expected Status command"),
    }
}

#[test]
fn test_cli_cancel_batch() {
    let cli = Cli::try_parse_from(["ingest", "cancel", "batch", "b_abc"]).unwrap();
    match cli.command {
        Commands::Cancel { scope } => match scope {
            CancelScope::Batch { batch_id } => assert_eq!(batch_id, "b_abc"),
            _ => panic!("expected Cancel batch"),
        },
        _ => panic!("expected Cancel command"),
    }
}

#[test]
fn test_cli_cancel_job() {
    let cli = Cli::try_parse_from(["ingest", "cancel", "job", "j_def"]).unwrap();
    match cli.command {
        Commands::Cancel { scope } => match scope {
            CancelScope::Job { job_id } => assert_eq!(job_id, "j_def"),
            _ => panic!("expected Cancel job"),
        },
        _ => panic!("expected Cancel command"),
    }
}

#[test]
fn test_cli_retry_job() {
    let cli = Cli::try_parse_from(["ingest", "retry", "job", "j_ghi"]).unwrap();
    match cli.command {
        Commands::Retry { scope } => match scope {
            RetryScope::Job { job_id } => assert_eq!(job_id, "j_ghi"),
        },
        _ => panic!("expected Retry command"),
    }
}

#[test]
fn test_cli_files_list_defaults() {
    let cli = Cli::try_parse_from(["ingest", "files", "list"]).unwrap();
    match cli.command {
        Commands::Files { scope } => match scope {
            FilesScope::List(args) => {
                assert!(args.mime.is_none());
                assert!(args.provider.is_none());
                assert!(args.from.is_none());
                assert!(args.to.is_none());
                assert_eq!(args.limit, 100);
                assert!(matches!(args.output, OutputFormat::Table));
            }
            _ => panic!("expected Files List"),
        },
        _ => panic!("expected Files command"),
    }
}

#[test]
fn test_cli_files_list_all_filters() {
    let cli = Cli::try_parse_from([
        "ingest",
        "files",
        "list",
        "--mime",
        "image/webp",
        "--provider",
        "local",
        "--from",
        "2024-01-01",
        "--to",
        "2024-12-31",
        "--limit",
        "50",
        "--output",
        "json",
    ])
    .unwrap();
    match cli.command {
        Commands::Files { scope } => match scope {
            FilesScope::List(args) => {
                assert_eq!(args.mime.unwrap(), "image/webp");
                assert_eq!(args.provider.unwrap(), "local");
                assert_eq!(args.from.unwrap(), "2024-01-01");
                assert_eq!(args.to.unwrap(), "2024-12-31");
                assert_eq!(args.limit, 50);
                assert!(matches!(args.output, OutputFormat::Json));
            }
            _ => panic!("expected Files List"),
        },
        _ => panic!("expected Files command"),
    }
}

#[test]
fn test_cli_files_get() {
    let cli = Cli::try_parse_from(["ingest", "files", "get", "abc123hash"]).unwrap();
    match cli.command {
        Commands::Files { scope } => match scope {
            FilesScope::Get { hash } => assert_eq!(hash, "abc123hash"),
            _ => panic!("expected Files Get"),
        },
        _ => panic!("expected Files command"),
    }
}

#[test]
fn test_cli_files_download_with_dest() {
    let cli =
        Cli::try_parse_from(["ingest", "files", "download", "hash123", "/tmp/output.bin"]).unwrap();
    match cli.command {
        Commands::Files { scope } => match scope {
            FilesScope::Download { hash, dest } => {
                assert_eq!(hash, "hash123");
                assert_eq!(dest.unwrap().to_string_lossy(), "/tmp/output.bin");
            }
            _ => panic!("expected Files Download"),
        },
        _ => panic!("expected Files command"),
    }
}

#[test]
fn test_cli_files_download_stdout() {
    let cli = Cli::try_parse_from(["ingest", "files", "download", "hash123"]).unwrap();
    match cli.command {
        Commands::Files { scope } => match scope {
            FilesScope::Download { hash, dest } => {
                assert_eq!(hash, "hash123");
                assert!(dest.is_none());
            }
            _ => panic!("expected Files Download"),
        },
        _ => panic!("expected Files command"),
    }
}

#[test]
fn test_cli_files_delete_with_yes() {
    let cli = Cli::try_parse_from(["ingest", "files", "delete", "hash123", "--yes"]).unwrap();
    match cli.command {
        Commands::Files { scope } => match scope {
            FilesScope::Delete { hash, yes } => {
                assert_eq!(hash, "hash123");
                assert!(yes);
            }
            _ => panic!("expected Files Delete"),
        },
        _ => panic!("expected Files command"),
    }
}

#[test]
fn test_cli_files_delete_no_flag() {
    let cli = Cli::try_parse_from(["ingest", "files", "delete", "hash123"]).unwrap();
    match cli.command {
        Commands::Files { scope } => match scope {
            FilesScope::Delete { hash, yes } => {
                assert_eq!(hash, "hash123");
                assert!(!yes);
            }
            _ => panic!("expected Files Delete"),
        },
        _ => panic!("expected Files command"),
    }
}

#[test]
fn test_cli_global_flags() {
    let cli = Cli::try_parse_from([
        "ingest",
        "--config",
        "/custom/config.toml",
        "--log-format",
        "json",
        "-v",
        "-q",
        "--no-color",
        "run",
        "test.yaml",
    ])
    .unwrap();
    assert_eq!(cli.config.to_string_lossy(), "/custom/config.toml");
    assert!(matches!(cli.global.log_format, LogFormat::Json));
    assert_eq!(cli.global.verbose, 1);
    assert!(cli.global.quiet);
    assert!(cli.global.no_color);
}

#[test]
fn test_cli_verbose_counts() {
    let cli = Cli::try_parse_from(["ingest", "-v", "run", "test.yaml"]).unwrap();
    assert_eq!(cli.global.verbose, 1);

    let cli = Cli::try_parse_from(["ingest", "-vv", "run", "test.yaml"]).unwrap();
    assert_eq!(cli.global.verbose, 2);

    let cli = Cli::try_parse_from(["ingest", "-vvv", "run", "test.yaml"]).unwrap();
    assert_eq!(cli.global.verbose, 3);
}

#[test]
fn test_cli_invalid_subcommand() {
    let result = Cli::try_parse_from(["ingest", "unknown", "test.yaml"]);
    assert!(result.is_err());
}

#[test]
fn test_cli_missing_required_arg() {
    let result = Cli::try_parse_from(["ingest", "run"]);
    assert!(result.is_err());
}

#[test]
fn test_job_status_serde() {
    let status = JobStatus::Pending;
    let json = serde_json::to_string(&status).unwrap();
    assert_eq!(json, "\"pending\"");
    let back: JobStatus = serde_json::from_str(&json).unwrap();
    assert_eq!(back, status);
}

#[test]
fn test_job_status_serde_all_variants() {
    for (input, expected) in [
        ("pending", JobStatus::Pending),
        ("running", JobStatus::Running),
        ("completed", JobStatus::Completed),
        ("failed", JobStatus::Failed),
        ("retrying", JobStatus::Retrying),
        ("cancelled", JobStatus::Cancelled),
    ] {
        let json = format!("\"{}\"", input);
        let deser: JobStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(deser, expected, "failed for {}", input);
    }
}

#[test]
fn test_output_format_default_table() {
    let cli = Cli::try_parse_from(["ingest", "run", "test.yaml"]).unwrap();
    match cli.command {
        Commands::Run(args) => assert!(matches!(args.output, OutputFormat::Table)),
        _ => panic!("expected Run"),
    }
}

#[test]
fn test_cli_help_does_not_panic() {
    let result = Cli::try_parse_from(["ingest", "--help"]);
    assert!(result.is_err());
}
