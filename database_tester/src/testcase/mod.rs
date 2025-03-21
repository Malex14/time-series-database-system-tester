use crate::metrics::MetricsRecorder;
use crate::scenario::Scenario;
use crate::vm::vm_manager::VMManager;
use log::{error, info};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use strum::IntoEnumIterator;
use strum_macros::{Display, EnumIter};
use tokio::time::sleep;

/// A test case that runs a scenario on a set of databases
/// and records the metrics of the queries.
pub struct Testcase {
	manager: VMManager,
	metrics_recorder: Arc<MetricsRecorder>,
	scenario: Box<dyn Scenario>,
	databases_to_test: Vec<Database>,
	times_to_run_each_query: usize,
	skip: usize,
	base_disk: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct SystemConfiguration {
	pub cpu_cores: u32,
	pub memory: u32,
}

#[derive(Debug, Clone, Display, PartialEq, EnumIter)]
pub enum Database {
	InfluxDB,
	TimescaleDB,
	MongoDB,
	PostgreSQL,
}

impl Testcase {
	const TEST_VM_NAME: &'static str = "tsdbs-vm";

	/// Creates a new test case.
	///
	/// # Arguments
	/// * `manager` - The VM manager to use for creating VMs.
	/// * `metrics_recorder` - The metrics recorder to use for recording the metrics.
	/// * `scenario` - The scenario to run.
	/// * `times_to_run_each_query` - The number of times to run each query.
	/// * `databases_to_test` - The databases to test. If `None`, all databases will be tested.
	/// * `skip` - The number of queries to skip.
	/// * `base_disk` - The base disk to use for the VMs. If `None`, a new disk will be created.
	pub fn new(
		manager: VMManager,
		metrics_recorder: Arc<MetricsRecorder>,
		scenario: Box<dyn Scenario>,
		times_to_run_each_query: usize,
		databases_to_test: impl Into<Option<Vec<Database>>>,
		skip: usize,
		base_disk: Option<impl AsRef<Path>>,
	) -> Self {
		Self {
			manager,
			metrics_recorder,
			scenario,
			databases_to_test: databases_to_test
				.into()
				.unwrap_or_else(|| Database::iter().collect::<Vec<_>>()),
			times_to_run_each_query,
			skip,
			base_disk: base_disk.map(|p| p.as_ref().to_path_buf()),
		}
	}

	/// Runs the test case.
	pub async fn run(&mut self) -> anyhow::Result<()> {
		info!(
			"Starting test case for scenario: {:?}",
			self.scenario.get_name()
		);

		for db in &self.databases_to_test {
			let base_disk = match self.base_disk {
				Some(ref p) => p.to_path_buf(),
				None => {
					info!("Initializing vm for dbs: {:?}", db);

					let mut disk_path = None;
					for config in self.scenario.get_init_system_configurations() {
						info!("Creating vm for testing databases");
						let vm = self.manager.create_vm(
							&format!(
								"{}-{db}-cpu-{}-mem-{}",
								Self::TEST_VM_NAME,
								config.cpu_cores,
								config.memory
							),
							config.cpu_cores,
							config.memory,
							true,
							&db.to_string(),
							None,
						)?;

						let rec = self
							.metrics_recorder
							.with_system_metrics_receiver_on_channel(&vm.get_channel())?;

						info!("Waiting for vm to get ip address");
						loop {
							sleep(Duration::from_secs(1)).await;
							if vm.get_ip_addr().is_some() {
								break;
							}
						}
						info!("VM got ip address: {:?}", vm.get_ip_addr().unwrap());

						info!("Initializing scenario");
						self.scenario
							.init(db, &vm.get_ip_addr().unwrap(), vm.get_password())
							.await?;
						info!("Scenario initialized");

						info!("Stopping vm");
						drop(rec);
						vm.stop()?;
						info!("Waiting for vm to stop");
						loop {
							sleep(Duration::from_secs(1)).await;
							if vm.get_state().is_err() {
								break;
							}
						}
						info!("VM stopped");

						info!("Using vm disk state as base");
						disk_path = Some(vm.get_disk_path().to_path_buf());
					}

					disk_path.expect("No disk path found")
				}
			};

			let base_disk = base_disk.as_path();

			info!("Starting tests for db: {}", db);

			let configs = self.scenario.get_system_configurations();
			for config in configs {
				info!("Testing with config: {:?}", config);

				for query in self.scenario.get_queries() {
					if self.skip > 0 {
						info!("Skipping query: {}", query);
						self.skip -= 1;
						continue;
					}

					info!("Running query: {}", query);

					let identifier = format!(
						"{db}-cpu-{}-mem-{}-{}",
						config.cpu_cores, config.memory, query
					);
					for i in 0..self.times_to_run_each_query {
						info!(
							"Creating vm for testing databases with config: {:?}",
							config
						);
						let vm = self.manager.create_vm(
							&format!("{}-{identifier}-{i}", Self::TEST_VM_NAME),
							config.cpu_cores,
							config.memory,
							true,
							&db.to_string(),
							Some(base_disk),
						)?;

						let rec = self
							.metrics_recorder
							.with_system_metrics_receiver_on_channel(&vm.get_channel())?;

						info!("Waiting for vm to get ip address");
						loop {
							sleep(Duration::from_secs(1)).await;
							if vm.get_ip_addr().is_some() {
								break;
							}
						}

						info!("Starting scenario");
						if let Err(error) = self
							.scenario
							.run(
								db,
								&identifier,
								query,
								&vm.get_ip_addr().unwrap(),
								vm.get_password(),
							)
							.await
						{
							error!("Error running scenario: {}", error);
						}
						info!("Scenario finished");

						info!("Stopping vm");
						drop(rec);
						vm.poweroff()?;

						info!("Waiting for vm to stop");
						loop {
							sleep(Duration::from_secs(1)).await;
							if vm.get_state().is_err() {
								break;
							}
						}
						info!("VM stopped");
					}

					info!("Exporting metrics for query: {}", query);
					self.metrics_recorder.save_measurement(&identifier)?;
				}
			}
			info!("Finished tests for db: {}", db);
		}

		info!(
			"Finished test case for scenario: {:?}",
			self.scenario.get_name()
		);

		Ok(())
	}
}
