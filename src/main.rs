pub mod bootstrap;
pub mod cli;
pub mod context;
pub mod handlers;
pub mod models;
pub mod services;
pub mod settings;
pub mod storage;
pub mod error;

use crate::{
    cli::{Commands, Global, LogFormat, get_config, load_config},
    error::ToolError,
    models::MainConfig,
    settings::merge_configs_yaml,
};
use colored::*;
use tracing_subscriber::{EnvFilter, filter::LevelFilter, layer::SubscriberExt, util::SubscriberInitExt};

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


    let builder = EnvFilter::builder()
        .with_default_directive(LevelFilter::WARN.into());

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

    let cli_config = match get_config() {
        Ok(config) => config,
        Err(e) => {
            println!("{} {}", "Error loading configuration:".red(), e);
            std::process::exit(e.exit_code());
        }
    };
    let global = cli_config.cli.global;

    setup_logging(global.clone());

    if is_pipe() {
        eprintln!("{}", "Detected piped output, auto-switching to JSON mode".yellow());
    }

    tracing::info!("Starting ingest CLI");

    let result = tokio::select! {
        result = async {
            match cli_config.cli.command {
                Commands::Run(run_args) => {
                    tracing::info!("Running ingestion...");
                    let yaml_path = run_args.yaml_path.clone();
                    let yaml_config = load_config(&yaml_path)?;

                    let toml_config = merge_configs_yaml(&yaml_config, cli_config.toml_config)?;

                    let config = MainConfig {
                        toml_config: toml_config,
                        yaml_config: yaml_config,
                        yaml_path: yaml_path,
                        redis_uri: cli_config.redis_uri,
                        mongo_uri: cli_config.mongo_uri,
                    };
                    bootstrap::run(config, run_args).await
                }
                Commands::Status { scope } => bootstrap::status(scope, cli_config.mongo_uri).await,
                Commands::Cancel { scope } => bootstrap::cancel(scope, cli_config.mongo_uri, cli_config.redis_uri).await,
                Commands::Retry { scope } =>  bootstrap::retry(scope, cli_config.mongo_uri).await,
                Commands::Files { scope } => bootstrap::files(scope, cli_config.mongo_uri).await,
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

