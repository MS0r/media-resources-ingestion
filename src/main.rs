pub mod bootstrap;
pub mod cli;
pub mod context;
pub mod handlers;
pub mod models;
pub mod services;
pub mod settings;
pub mod storage;
pub mod error;

use crate::{cli::{Commands, load_config}, error::BoxedError, models::MainConfig, settings::{merge_configs_cli, merge_configs_yaml}};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<(), BoxedError> {
    dotenvy::dotenv().ok();

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| format!("{}=debug", env!("CARGO_PKG_NAME")).into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    tracing::info!("Starting ingest CLI");

    let cli_config = cli::get_config()?;
    match cli_config.cli.command {
        Commands::Run(run_args) => {
            tracing::info!("Running ingestion...");
            let config_path = run_args.yaml_path.clone();
            let yaml_config = load_config(&config_path)?;

            let yaml_merge = merge_configs_yaml(&yaml_config, cli_config.toml_config)?;
            let toml_config = merge_configs_cli(cli_config.cli.global, run_args, yaml_merge)?;

            let config = MainConfig {
                toml_config: toml_config,
                yaml_config: yaml_config,
                yaml_path: config_path,
                redis_uri: cli_config.redis_uri,
                mongo_uri: cli_config.mongo_uri,
            };
            bootstrap::run(config).await?;
        },
        Commands::Status { scope } => bootstrap::status(scope).await?,
        Commands::Cancel { scope } => bootstrap::cancel(scope).await?,
        Commands::Retry { scope } =>  bootstrap::retry(scope).await?,
        Commands::Files { scope} => bootstrap::files(scope).await?,
    }
    return Ok(());
}