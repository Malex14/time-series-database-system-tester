mod db;
pub mod scenario_a;
pub mod scenario_b;
pub mod scenario_c;

use crate::testcase::{Database, SystemConfiguration};
use async_trait::async_trait;
use std::net::IpAddr;

/// Trait for a scenario that can be run by the database tester.
#[async_trait]
pub trait Scenario {
	fn get_name(&self) -> &'static str;
	fn get_queries(&self) -> &'static [&'static str];
	fn get_init_system_configurations(&self) -> &'static [SystemConfiguration] {
		&[SystemConfiguration {
			cpu_cores: 8,
			memory: 16384,
		}]
	}
	fn get_system_configurations(&self) -> &'static [SystemConfiguration];

	async fn init(
		&self,
		database: &Database,
		ip_addr: &IpAddr,
		password: Option<&str>,
	) -> anyhow::Result<()>;

	async fn run(
		&self,
		database: &Database,
		run_identifier: &str,
		query: &str,
		ip_addr: &IpAddr,
		password: Option<&str>,
	) -> anyhow::Result<()>;
}

#[macro_export]
macro_rules! impl_get_connection {
	() => {
		async fn get_connection(
			&self,
			database: &Database,
			ip_addr: &IpAddr,
			password: Option<&str>,
		) -> anyhow::Result<Box<dyn DatabaseConnection + Send + 'static>> {
			loop {
				info!("Trying to connect to database");

				let maybe_con: anyhow::Result<Box<dyn DatabaseConnection + Send + 'static>> =
					match database {
						Database::InfluxDB => DatabaseConnectionInfluxDB::new(
							&format!("http://{}:8086", ip_addr),
							password.ok_or(anyhow::anyhow!("InfluxDB requires a password"))?,
						)
						.await
						.map(|c| Box::new(c) as Box<dyn DatabaseConnection + Send + 'static>),
						Database::TimescaleDB => DatabaseConnectionTimescaleDB::new(&format!(
							"postgres://postgres@{}:5432/postgres",
							ip_addr
						))
						.await
						.map(|c| Box::new(c) as Box<dyn DatabaseConnection + Send + 'static>),
						Database::MongoDB => {
							DatabaseConnectionMongoDB::new(&format!("mongodb://{}:27017", ip_addr))
								.await
								.map(|c| {
									Box::new(c) as Box<dyn DatabaseConnection + Send + 'static>
								})
						}
						Database::PostgreSQL => DatabaseConnectionPostgres::new(&format!(
							"postgres://postgres@{}:5432/postgres",
							ip_addr
						))
						.await
						.map(|c| Box::new(c) as Box<dyn DatabaseConnection + Send + 'static>),
					};

				match maybe_con {
					Ok(con) => return Ok(con),
					Err(e) => {
						info!("Error connecting to database: {}", e);
					}
				}

				tokio::time::sleep(std::time::Duration::from_secs(10)).await;
			}
		}
	};
}
