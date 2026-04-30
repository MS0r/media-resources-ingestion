mod bootstrap;
mod cli;
mod context;
mod handlers;
mod models;
mod services;
mod settings;
mod storage;
mod error;

use crate::error::BoxedError;
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

    let config = cli::get_config()?;
    bootstrap::run(config).await
}