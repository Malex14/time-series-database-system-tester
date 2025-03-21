mod db;
mod generator;

use crate::impl_get_connection;
use crate::metrics::MetricsRecorder;
use crate::scenario::scenario_a::db::DatabaseConnectionInfluxDB;
use crate::scenario::scenario_a::db::DatabaseConnectionMongoDB;
use crate::scenario::scenario_a::db::DatabaseConnectionPostgres;
use crate::scenario::scenario_a::db::DatabaseConnectionTimescaleDB;
use crate::scenario::scenario_a::generator::{
	DoorWindowEvent, DoorWindowGenerator, LightGenerator, PowerGenerator, TemperatureGenerator,
};
use crate::scenario::Scenario;
use crate::testcase::{Database, SystemConfiguration};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::Stream;
use futures_util::{stream, StreamExt};
use log::{error, info};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use sqlx::Postgres;
use std::fmt::Display;
use std::future::Future;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::TryRecvError;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_stream::StreamExt as TokioStreamExt;

#[async_trait]
trait DatabaseConnection {
	async fn initialize_schema(&mut self) -> anyhow::Result<()>;

	async fn count_doors_windows_open(&mut self) -> anyhow::Result<()>;
	async fn total_energy(&mut self) -> anyhow::Result<()>;
	async fn average_temperature_per_min(&mut self) -> anyhow::Result<()>;
}

#[async_trait]
trait DeviceDatabaseConnection<T: Send + 'static> {
	async fn write_data(
		&mut self,
		data: Box<dyn Stream<Item = Record<T>> + Send + 'static>,
		metrics_recorder: Arc<MetricsRecorder>,
	) -> anyhow::Result<()>;
}

#[derive(Debug, Serialize, Deserialize)]
struct Record<T> {
	time: DateTime<Utc>,
	category: String,
	id: i64,
	value: T,
}

pub struct ScenarioA {
	metrics_recorder: Arc<MetricsRecorder>,
}

impl ScenarioA {
	const SEED: u64 = 98433732;

	// RATEs are in seconds
	const ADD_GENERATOR_RATE: u64 = 3;
	const MAX_GENERATORS: usize = 400;
	const TIMES_TO_RUN_QUERY: usize = 1000;

	const POWER_GENERATOR_RATE: u64 = 1;
	const TEMPERATURE_GENERATOR_RATE: u64 = 2;
	const DOORS_AND_WINDOWS_GENERATOR_RATE: u64 = 1;
	const LIGHTS_GENERATOR_RATE: u64 = 1;

	const TEMPERATURE_GENERATOR_PERIOD: usize = 24 * 60 * 60;
	const TEMPERATURE_GENERATOR_DAY_NIGHT_DIFFERENCE: f64 = 4.0;
	const TEMPERATURE_GENERATOR_SHIFT: f64 = 0.04;
	const TEMPERATURE_GENERATOR_OFFSET: f64 = 17.0;
	const TEMPERATURE_GENERATOR_DISTORTION: f64 = 0.0;

	const DOOR_OR_WINDOW_EVENT_PROBABILITY: f64 = 1. / 50.;
	const LIGHT_EVENT_PROBABILITY: f64 = 1. / 50.;

	pub fn new(metrics_recorder: Arc<MetricsRecorder>) -> Self {
		Self { metrics_recorder }
	}

	impl_get_connection!();

	async fn get_device_connection<T>(
		database: &Database,
		ip_addr: &IpAddr,
		password: Option<&str>,
	) -> anyhow::Result<Box<dyn DeviceDatabaseConnection<T> + Send + 'static>>
	where
		T: Send + Sync + Serialize + Display + sqlx::Type<Postgres> + Into<bson::Bson> + 'static,
		for<'q> T: sqlx::Encode<'q, Postgres>,
	{
		match database {
			Database::InfluxDB => DatabaseConnectionInfluxDB::new(
				&format!("http://{}:8086", ip_addr),
				password.ok_or(anyhow::anyhow!("InfluxDB requires a password"))?,
			)
			.await
			.map(|c| Box::new(c) as Box<dyn DeviceDatabaseConnection<T> + Send + 'static>),
			Database::TimescaleDB => DatabaseConnectionTimescaleDB::new(&format!(
				"postgres://postgres@{}:5432/postgres",
				ip_addr
			))
			.await
			.map(|c| Box::new(c) as Box<dyn DeviceDatabaseConnection<T> + Send + 'static>),
			Database::MongoDB => {
				DatabaseConnectionMongoDB::new(&format!("mongodb://{}:27017", ip_addr))
					.await
					.map(|c| Box::new(c) as Box<dyn DeviceDatabaseConnection<T> + Send + 'static>)
			}
			Database::PostgreSQL => DatabaseConnectionPostgres::new(&format!(
				"postgres://postgres@{}:5432/postgres",
				ip_addr
			))
			.await
			.map(|c| Box::new(c) as Box<dyn DeviceDatabaseConnection<T> + Send + 'static>),
		}
	}

	#[allow(clippy::too_many_arguments)]
	async fn create_generator_from_iter<T, U, F, Fut>(
		gen: impl Iterator<Item = T> + Send + 'static,
		category_name: &str,
		id: i64,
		sample_rate: u64,
		stop: broadcast::Receiver<()>,
		database: &Database,
		ip_addr: &IpAddr,
		password: Option<&str>,
		metrics_recorder: Arc<MetricsRecorder>,
		filter_map_fn: F,
	) -> anyhow::Result<JoinHandle<anyhow::Result<()>>>
	where
		U: Send + Sync + Serialize + Display + sqlx::Type<Postgres> + Into<bson::Bson> + 'static,
		for<'q> U: sqlx::Encode<'q, Postgres>,
		F: (FnMut(T) -> Fut) + Send + 'static,
		Fut: Future<Output = Option<U>> + Send + 'static,
	{
		let mut con = Self::get_device_connection::<U>(database, ip_addr, password).await?;
		let category_name = category_name.to_string();
		Ok(tokio::spawn(async move {
			let stream = futures_util::stream::unfold(
				(
					Box::into_pin(Box::new(
						stream::iter(gen).throttle(Duration::from_millis(sample_rate)),
					)), // stream
					Instant::now(), // start_time
					None,           // stopping
					0,              // generated
					stop,
					sample_rate,
				),
				|(mut stream, start_time, mut stopping, generated, mut stop, sample_rate)| async move {
					match StreamExt::next(&mut stream).await {
						Some(value) => {
							if stopping.is_none()
								&& stop.try_recv().err() != Some(TryRecvError::Empty)
							{
								stopping =
									Some(start_time.elapsed().as_millis() / sample_rate as u128);
							}

							if let Some(should_have_generated) = stopping {
								return if should_have_generated > generated {
									Some((
										value,
										(
											stream,
											start_time,
											stopping,
											generated + 1,
											stop,
											sample_rate,
										),
									))
								} else {
									None
								};
							}

							Some((
								value,
								(
									stream,
									start_time,
									stopping,
									generated + 1,
									stop,
									sample_rate,
								),
							))
						}
						None => None,
					}
				},
			);

			con.write_data(
				Box::new(StreamExt::map(
					StreamExt::filter_map(stream, filter_map_fn),
					move |value| Record {
						time: Utc::now(),
						category: category_name.clone(),
						id,
						value,
					},
				)),
				metrics_recorder,
			)
			.await?;

			Ok(())
		}))
	}
}

#[async_trait]
impl Scenario for ScenarioA {
	fn get_name(&self) -> &'static str {
		"Szenario A: Smart Home"
	}

	fn get_queries(&self) -> &'static [&'static str] {
		&[
			"count_doors_windows_open",
			"total_energy",
			"average_temperature_per_min",
		]
	}

	fn get_init_system_configurations(&self) -> &'static [SystemConfiguration] {
		&[
			SystemConfiguration {
				cpu_cores: 1,
				memory: 512,
			},
			SystemConfiguration {
				cpu_cores: 2,
				memory: 512,
			},
			SystemConfiguration {
				cpu_cores: 1,
				memory: 1024,
			},
			SystemConfiguration {
				cpu_cores: 2,
				memory: 1024,
			},
		]
	}

	fn get_system_configurations(&self) -> &'static [SystemConfiguration] {
		&[
			SystemConfiguration {
				cpu_cores: 1,
				memory: 512,
			},
			SystemConfiguration {
				cpu_cores: 2,
				memory: 512,
			},
			SystemConfiguration {
				cpu_cores: 1,
				memory: 1024,
			},
			SystemConfiguration {
				cpu_cores: 2,
				memory: 1024,
			},
		]
	}

	async fn init(
		&self,
		database: &Database,
		ip_addr: &IpAddr,
		password: Option<&str>,
	) -> anyhow::Result<()> {
		{
			let mut con = self.get_connection(database, ip_addr, password).await?;
			info!("Initializing schema");
			sleep(Duration::from_secs(20)).await;
			con.initialize_schema().await?;
			drop(con);
		}

		let mut generators: Vec<JoinHandle<anyhow::Result<()>>> = Vec::new();
		let mut rand = SmallRng::seed_from_u64(Self::SEED);

		let mut power_id = 0;
		let mut temperature_id = 0;
		let mut doors_and_windows_id = 0;
		let mut lights_id = 0;

		let (stopper, stop_receiver) = broadcast::channel(1);

		let measurement_name = format!("{database}-init-{}", Utc::now().timestamp());
		self.metrics_recorder.start_measurement(&measurement_name);

		let gen_res = async {
			while generators.len() < Self::MAX_GENERATORS {
				generators.push(match rand.random_range(0..4) {
					0 => {
						// Power generator
						power_id += 1;
						Self::create_generator_from_iter(
							PowerGenerator::get_random_device(&mut rand),
							"power",
							power_id,
							Self::POWER_GENERATOR_RATE * 1000,
							stop_receiver.resubscribe(),
							database,
							ip_addr,
							password,
							self.metrics_recorder.clone(),
							|v| async move { Some(v) },
						)
						.await?
					}
					1 => {
						// Temperature generator
						temperature_id += 1;
						Self::create_generator_from_iter(
							TemperatureGenerator::new(
								Self::TEMPERATURE_GENERATOR_PERIOD,
								Self::TEMPERATURE_GENERATOR_DAY_NIGHT_DIFFERENCE,
								Self::TEMPERATURE_GENERATOR_SHIFT
									+ rand.random_range(0.0..(60.0 * 60.0)), // 0..60 minutes
								Self::TEMPERATURE_GENERATOR_OFFSET,
								Self::TEMPERATURE_GENERATOR_DISTORTION,
								0.5,
								rand.random(),
							),
							"temperature",
							temperature_id,
							Self::TEMPERATURE_GENERATOR_RATE * 1000,
							stop_receiver.resubscribe(),
							database,
							ip_addr,
							password,
							self.metrics_recorder.clone(),
							|v| async move { Some(v) },
						)
						.await?
					}
					2 => {
						// Doors and windows generator
						doors_and_windows_id += 1;
						Self::create_generator_from_iter(
							DoorWindowGenerator::new(
								Self::DOOR_OR_WINDOW_EVENT_PROBABILITY,
								rand.random(),
							)
							.map(|o| o.map(|e| e == DoorWindowEvent::Open)),
							"doors_and_windows",
							doors_and_windows_id,
							Self::DOORS_AND_WINDOWS_GENERATOR_RATE * 1000,
							stop_receiver.resubscribe(),
							database,
							ip_addr,
							password,
							self.metrics_recorder.clone(),
							|v| async move { v },
						)
						.await?
					}
					3 => {
						// Lights generator
						lights_id += 1;
						Self::create_generator_from_iter(
							LightGenerator::new(Self::LIGHT_EVENT_PROBABILITY, rand.random()),
							"lights",
							lights_id,
							Self::LIGHTS_GENERATOR_RATE * 1000,
							stop_receiver.resubscribe(),
							database,
							ip_addr,
							password,
							self.metrics_recorder.clone(),
							|v| async move { v },
						)
						.await?
					}
					_ => unreachable!(),
				});
				info!("Added generator. Now {} generators", generators.len());

				sleep(Duration::from_millis(
					Self::ADD_GENERATOR_RATE * 1000 + rand.random_range(0..1000) - 500,
				))
				.await
			}
			Ok::<_, anyhow::Error>(())
		}
		.await;

		if let Err(error) = gen_res {
			error!("Error running generator: {}", error);
			for task in &generators {
				task.abort();
			}
		}

		info!("Sending stop signal");
		stopper.send(())?;

		info!("Waiting for generators to finish");
		for task in generators {
			let res = task.await;
			match res {
				Ok(Err(error)) => {
					error!("Error running generator: {}", error);
				}
				Err(error) if !error.is_cancelled() => {
					error!("Generator panicked: {}", error);
				}
				_ => {}
			}
		}

		self.metrics_recorder.stop_measurement();
		self.metrics_recorder.save_measurement(&measurement_name)?;

		Ok(())
	}

	async fn run(
		&self,
		database: &Database,
		run_identifier: &str,
		query: &str,
		ip_addr: &IpAddr,
		password: Option<&str>,
	) -> anyhow::Result<()> {
		let mut connection = self.get_connection(database, ip_addr, password).await?;

		self.metrics_recorder.start_measurement(run_identifier);
		for _ in 0..Self::TIMES_TO_RUN_QUERY {
			let start = Instant::now();
			match query {
				"count_doors_windows_open" => {
					connection.count_doors_windows_open().await?;
				}
				"total_energy" => {
					connection.total_energy().await?;
				}
				"average_temperature_per_min" => {
					connection.average_temperature_per_min().await?;
				}
				_ => unreachable!(),
			}
			let duration = start.elapsed();
			self.metrics_recorder.add_latency_metric(duration);
		}
		self.metrics_recorder.stop_measurement();

		Ok(())
	}
}
