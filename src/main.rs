pub mod bootstrap;
pub mod cli;
pub mod context;
pub mod error;
pub mod handlers;
pub mod models;
pub mod services;
pub mod settings;
pub mod storage;

use crate::{
    cli::{Cli, Commands, Global, LogFormat, load_config, load_env_uris},
    error::ToolError,
    models::AppConfig,
    settings::load_toml,
};
use clap::Parser;
use colored::*;
use tracing_subscriber::{
    EnvFilter, filter::LevelFilter, layer::SubscriberExt, util::SubscriberInitExt,
};

fn init_ffmpeg() {
    ffmpeg_next::init().ok();
}

fn setup_logging(global: Global) {
    use tracing_subscriber::fmt::format;

    let log_format = match std::env::var("LOG_FORMAT").ok() {
        Some(s) if s.to_lowercase() == "json" => LogFormat::Json,
        _ => global.log_format,
    };

    let verbosity = std::env::var("INGEST_VERBOSE")
        .ok()
        .and_then(|v| v.parse::<u8>().ok())
        .unwrap_or(global.verbose);

    let builder = EnvFilter::builder().with_default_directive(LevelFilter::WARN.into());

    let filter: EnvFilter = if global.quiet {
        builder
            .parse(format!("{}=error", env!("CARGO_PKG_NAME").to_lowercase()).as_str())
            .expect("invalid filter directive")
    } else if verbosity > 0 {
        let level = match verbosity {
            1 => LevelFilter::INFO,
            2 => LevelFilter::DEBUG,
            _ => LevelFilter::TRACE,
        };
        builder
            .parse(format!("{}={}", env!("CARGO_PKG_NAME").to_lowercase(), level).as_str())
            .expect("invalid filter directive")
    } else {
        builder.from_env_lossy()
    };

    match log_format {
        LogFormat::Json => {
            tracing_subscriber::registry()
                .with(filter)
                .with(tracing_subscriber::fmt::layer().json())
                .init();
        }
        LogFormat::Pretty => {
            let fmt = format().with_ansi(!global.no_color);
            tracing_subscriber::registry()
                .with(filter)
                .with(tracing_subscriber::fmt::layer().event_format(fmt))
                .init();
        }
    }
}

fn is_pipe() -> bool {
    !atty::is(atty::Stream::Stdout)
}

#[tokio::main]
async fn main() -> Result<(), ToolError> {
    dotenvy::dotenv().ok();

    let cli = Cli::parse();
    let global = cli.global.clone();

    setup_logging(global.clone());
    init_ffmpeg();

    let (redis_uri, mongo_uri) = match load_env_uris() {
        Ok(uris) => uris,
        Err(e) => {
            println!("{} {}", "Error loading configuration:".red(), e);
            std::process::exit(e.exit_code());
        }
    };

    let toml_config = match load_toml(&cli.config) {
        Ok(cfg) => cfg,
        Err(e) => {
            println!("{} {}", "Error loading configuration:".red(), e);
            std::process::exit(e.exit_code());
        }
    };

    if is_pipe() {
        eprintln!(
            "{}",
            "Detected piped output, auto-switching to JSON mode".yellow()
        );
    }

    tracing::info!("Starting ingest CLI");

    let result = tokio::select! {
        result = async {
            match cli.command {
                Commands::Run(run_args) => {
                    tracing::info!("Running ingestion...");
                    let yaml_config = load_config(&run_args.yaml_path)?;

                    let config = AppConfig::from_sources(
                        toml_config,
                        &yaml_config,
                        &run_args,
                        redis_uri,
                        mongo_uri,
                    );

                    bootstrap::run(config, &yaml_config).await
                }
                Commands::Enqueue(enqueue_args) => {
                    tracing::info!("Enqueuing jobs...");
                    let yaml_config = load_config(&enqueue_args.yaml_path)?;

                    let config = AppConfig::from_enqueue_args(
                        toml_config,
                        &yaml_config,
                        &enqueue_args,
                        redis_uri,
                        mongo_uri,
                    );

                    let batch_id = bootstrap::enqueue(&config, &yaml_config).await?;
                    println!("Batch ID: {}", batch_id);
                    Ok(())
                }
                Commands::Worker(worker_args) => {
                    tracing::info!("Starting standalone worker...");
                    let config = AppConfig::from_toml_env(
                        toml_config,
                        redis_uri,
                        mongo_uri,
                        worker_args.workers,
                    );
                    bootstrap::worker(config).await
                }
                Commands::Status { scope } => bootstrap::status(scope, mongo_uri).await,
                Commands::Cancel { scope } => bootstrap::cancel(scope, mongo_uri, redis_uri).await,
                Commands::Retry { scope } =>  bootstrap::retry(scope, mongo_uri).await,
                Commands::Files { scope } => bootstrap::files(scope, mongo_uri).await,
            }
        } => result,
        _ = tokio::signal::ctrl_c() => {
            eprintln!("{}", "Interrupted by SIGINT (Ctrl+C)".red());
            return Err(ToolError::Interrupted);
        }
    };

    match result {
        Ok(_) => Ok(()),
        Err(e) => {
            let code = e.exit_code();
            if global.quiet {
                eprintln!("Error: {}", e);
            } else {
                eprintln!("{} {}", "Error:".red(), e);
            }
            std::process::exit(code);
        }
    }
}
