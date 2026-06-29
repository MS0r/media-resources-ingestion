use std::{net::SocketAddr, env::var as env_var};

use tracing_subscriber::{registry, fmt::layer};
use ingest_core::ToolError;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

fn init_ffmpeg() {
    ffmpeg_next::init().ok();
}

fn setup_logging() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let registry = registry().with(filter);
    let use_json = env_var("LOG_FORMAT").map_or(false, |v| v.to_lowercase() == "json");

    if use_json {
        registry.with(layer().json()).init();
    } else {
        registry.with(layer().pretty()).init();
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

    let toml_path = env_var("INGEST_CONFIG").unwrap_or_else(|_| ".ingest/config.toml".into());

    ingest_core::server::serve(addr, std::path::Path::new(&toml_path)).await
}
