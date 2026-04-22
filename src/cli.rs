use crate::models::IngestionRequest;
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "media-resources-ingestion")]
#[command(about = "Media resources ingestion CLI tool", long_about = None)]
struct Cli {
    #[arg(short, long, value_name = "FILE")]
    config: PathBuf,
}

pub fn load_config(path: &str) -> Result<IngestionRequest, Box<dyn std::error::Error>> {
    let content = std::fs::read_to_string(path)?;
    let request: IngestionRequest = serde_yaml::from_str(&content)?;
    Ok(request)
}

pub fn get_config_path() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    Ok(cli.config)
}