use crate::models::TomlConfig;
use std::path::PathBuf;


pub fn load_config(path : PathBuf) -> Result<TomlConfig, Box<dyn std::error::Error>> {
    let toml_fs = std::fs::read_to_string(path)?;
    let config: TomlConfig = toml::from_str(&toml_fs)?;
    Ok(config)
}