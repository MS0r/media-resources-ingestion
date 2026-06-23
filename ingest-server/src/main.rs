use std::net::SocketAddr;

use ingest_core::ToolError;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

fn init_ffmpeg() {
    ffmpeg_next::init().ok();
}

fn setup_logging() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let use_json = std::env::var("LOG_FORMAT").map_or(false, |v| v.to_lowercase() == "json");

    if use_json {
        tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer().json())
            .init();
    } else {
        tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer().pretty())
            .init();
    }
}

#[tokio::main]
async fn main() -> Result<(), ToolError> {
    dotenvy::dotenv().ok();
    setup_logging();
    init_ffmpeg();

    let addr: SocketAddr = std::env::var("INGEST_SERVER_ADDR")
        .unwrap_or_else(|_| "[::1]:50051".into())
        .parse()
        .map_err(|e| ToolError::ConfigError(format!("invalid INGEST_SERVER_ADDR: {e}")))?;

    let toml_path = std::env::var("INGEST_CONFIG").unwrap_or_else(|_| ".ingest/config.toml".into());

    ingest_core::server::serve(addr, std::path::Path::new(&toml_path)).await
}
