use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemMetrics {
	pub cpu_usage: f64,
	pub ram_used: u64,
	pub ram_total: u64,
	pub disk_used: u64,
	pub disk_total: u64,
	pub disk_read: u64,
	pub disk_write: u64,
	pub network_in: u64,
	pub network_out: u64,
}

impl FromStr for SystemMetrics {
	type Err = serde_json::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		serde_json::from_str(s)
	}
}

#[derive(Debug, Clone)]
pub struct EnergyMetrics {
	pub power_usage: f64,
}
