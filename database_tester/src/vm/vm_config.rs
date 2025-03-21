use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize, Clone)]
pub struct VMConfig {
	pub temp_dir: PathBuf,
	pub disk_size: String,
	pub base_disk_image: PathBuf,
	pub network: NetworkConfig,
	pub database: Vec<DatabaseConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct NetworkConfig {
	pub name: String,
	pub host_ip: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseConfig {
	pub name: String,
	pub user: Option<String>,
	pub password: Option<String>,
	pub init_commands: String,
}

impl VMConfig {
	pub fn read_config(path: impl AsRef<Path>) -> anyhow::Result<Self> {
		let config = std::fs::read_to_string(path)?;
		let mut config: VMConfig = toml::from_str(&config)?;

		config.temp_dir = config.temp_dir.canonicalize()?;

		Ok(config)
	}
}
