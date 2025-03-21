#![feature(sync_unsafe_cell)]

mod metrics;
mod scenario;
mod testcase;
mod vm;

use crate::metrics::MetricsRecorder;
use crate::scenario::scenario_a::ScenarioA;
use crate::scenario::scenario_b::ScenarioB;
use crate::scenario::scenario_c::ScenarioC;
use crate::scenario::Scenario;
#[allow(unused_imports)]
use crate::testcase::Database::{InfluxDB, MongoDB, PostgreSQL, TimescaleDB};
use crate::testcase::Testcase;
use crate::vm::vm_manager::VMManager;
use clap::Parser;
use env_logger::Env;
use log::info;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
	#[arg(short, long, help = "The scenario to run. Example: ScenarioA")]
	scenario: String,
	#[arg(short, long, help = "Run each query this many times")]
	times_to_run_each_query: usize,
}

#[tokio::main]
async fn main() {
	dotenvy::dotenv().ok();
	env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

	let args = Args::parse();

	info!("Starting database tester");

	info!("Connecting to hypervisor");
	let manager = VMManager::new("./vm_config.toml", None).await.unwrap();
	info!(
		"Connection to hypervisor established: {:?}",
		manager.get_connection()
	);

	info!("Starting metrics recorder");
	let recorder = MetricsRecorder::new("./out", Duration::from_millis(200)).unwrap();
	info!("Metrics recorder started");

	let scenario: Box<dyn Scenario> = match args.scenario.as_str() {
		"ScenarioA" => Box::new(ScenarioA::new(recorder.clone())),
		"ScenarioB" => Box::new(ScenarioB::new("./dataset/scenario_b", recorder.clone())),
		"ScenarioC" => Box::new(ScenarioC::new(recorder.clone())),
		_ => {
			info!("Invalid scenario specified");
			return;
		}
	};

	info!("Starting test case");
	let mut testcase = Testcase::new(
		manager,
		recorder,
		scenario,
		args.times_to_run_each_query,
		None,
		0,
		None::<&str>,
	);
	testcase.run().await.unwrap();
}
