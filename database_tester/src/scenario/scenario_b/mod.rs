use crate::impl_get_connection;
use crate::metrics::MetricsRecorder;
use crate::scenario::scenario_b::db::{
	DatabaseConnectionInfluxDB, DatabaseConnectionMongoDB, DatabaseConnectionPostgres,
	DatabaseConnectionTimescaleDB,
};
use crate::scenario::Scenario;
use crate::testcase::{Database, SystemConfiguration};
use arrow_array::{Array, Float64Array, Int64Array, TimestampMicrosecondArray};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use log::info;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use std::vec;

mod db;

#[async_trait]
trait DatabaseConnection {
	async fn initialize_schema(&mut self) -> anyhow::Result<()>;
	async fn import_data(
		&mut self,
		data: Box<dyn Iterator<Item = Record> + Send + 'static>,
	) -> anyhow::Result<()>;

	async fn query_average_fare_price_per_month(&mut self) -> anyhow::Result<()>;
	async fn query_passenger_counts(&mut self) -> anyhow::Result<()>;
	async fn query_all_fare_distances_higher_than_20(&mut self) -> anyhow::Result<()>;
	async fn query_hour_with_most_fares(&mut self) -> anyhow::Result<()>;
}

pub struct ScenarioB {
	dataset_dir: PathBuf,
	metrics_recorder: Arc<MetricsRecorder>,
}

impl ScenarioB {
	pub fn new(dataset_dir: impl AsRef<Path>, metrics_recorder: Arc<MetricsRecorder>) -> Self {
		Self {
			dataset_dir: dataset_dir.as_ref().to_path_buf(),
			metrics_recorder,
		}
	}

	impl_get_connection!();
}

#[async_trait]
impl Scenario for ScenarioB {
	fn get_name(&self) -> &'static str {
		"Szenario B: Taxen in New York City"
	}

	fn get_queries(&self) -> &'static [&'static str] {
		&[
			"average_fare_price_per_month",
			"passenger_counts",
			"all_fare_distances_higher",
			"hour_with_most_fares",
		]
	}

	fn get_system_configurations(&self) -> &'static [SystemConfiguration] {
		&[
			SystemConfiguration {
				cpu_cores: 4,
				memory: 16384,
			},
			SystemConfiguration {
				cpu_cores: 8,
				memory: 16384,
			},
			SystemConfiguration {
				cpu_cores: 16,
				memory: 16384,
			},
		]
	}

	async fn init(
		&self,
		database: &Database,
		ip_addr: &IpAddr,
		password: Option<&str>,
	) -> anyhow::Result<()> {
		let mut connection = self.get_connection(database, ip_addr, password).await?;

		connection.initialize_schema().await?;
		connection
			.import_data(Box::new(ParquetReader::new(&self.dataset_dir)?))
			.await?;

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
		let start = Instant::now();
		match query {
			"average_fare_price_per_month" => {
				connection.query_average_fare_price_per_month().await?;
			}
			"passenger_counts" => {
				connection.query_passenger_counts().await?;
			}
			"all_fare_distances_higher" => {
				connection.query_all_fare_distances_higher_than_20().await?;
			}
			"hour_with_most_fares" => {
				connection.query_hour_with_most_fares().await?;
			}
			_ => unreachable!(),
		}
		let duration = start.elapsed();
		self.metrics_recorder.add_latency_metric(duration);
		self.metrics_recorder.stop_measurement();

		Ok(())
	}
}

#[derive(Debug, Serialize, Deserialize)]
struct Record {
	#[serde(with = "bson::serde_helpers::chrono_datetime_as_bson_datetime")]
	tpep_pickup_datetime: DateTime<Utc>,
	passenger_count: i64,
	trip_distance: f64,
	total_amount: f64,
}

/// A reader for parquet files containing the taxi data.
///
/// The reader reads the data in batches and returns the records one by one as an iterator.
struct ParquetReader {
	files: vec::IntoIter<PathBuf>,
	reader: Option<ParquetRecordBatchReader>,
	batch: VecDeque<Record>,
}

impl ParquetReader {
	pub fn new(dir: &Path) -> anyhow::Result<Self> {
		let files = std::fs::read_dir(dir)?
			.map(|entry| entry.unwrap().path())
			.filter(|entry| entry.is_file())
			.filter(|entry| entry.extension().map_or(false, |ext| ext == "parquet"))
			.collect::<Vec<_>>();

		let mut s = Self {
			files: files.into_iter(),
			reader: None,
			batch: VecDeque::new(),
		};
		s.set_next_reader()?;

		Ok(s)
	}

	fn set_next_reader(&mut self) -> anyhow::Result<Option<()>> {
		if let Some(file) = self.files.next() {
			info!("Reading file: {:?}", file);
			let file = std::fs::File::open(file)?;
			self.reader = Some(ParquetRecordBatchReaderBuilder::try_new(file)?.build()?);
			Ok(Some(()))
		} else {
			self.reader = None;
			Ok(None)
		}
	}
}

impl Iterator for ParquetReader {
	type Item = Record;

	fn next(&mut self) -> Option<Self::Item> {
		let reader = self.reader.as_mut()?;

		if self.batch.is_empty() {
			if let Some(batch) = reader.next() {
				let batch = batch.ok()?;
				let passenger_count = batch.column_by_name("passenger_count")?.as_any();
				let passenger_count =
					if let Some(passenger_count) = passenger_count.downcast_ref::<Float64Array>() {
						Box::new(passenger_count.values().iter().map(|v| *v as i64))
							as Box<dyn Iterator<Item = i64>>
					} else {
						Box::new(
							passenger_count
								.downcast_ref::<Int64Array>()
								.unwrap()
								.values()
								.iter()
								.copied(),
						) as Box<dyn Iterator<Item = i64>>
					};
				let tpep_pickup_datetime = batch
					.column_by_name("tpep_pickup_datetime")?
					.as_any()
					.downcast_ref::<TimestampMicrosecondArray>()
					.unwrap()
					.values()
					.iter();
				let trip_distance = batch
					.column_by_name("trip_distance")?
					.as_any()
					.downcast_ref::<Float64Array>()
					.unwrap()
					.values()
					.iter();
				let total_amount = batch
					.column_by_name("total_amount")?
					.as_any()
					.downcast_ref::<Float64Array>()
					.unwrap()
					.values()
					.iter();

				self.batch.extend(
					tpep_pickup_datetime
						.zip(passenger_count)
						.zip(trip_distance)
						.zip(total_amount)
						.map(
							|(
								((tpep_pickup_datetime, passenger_count), trip_distance),
								total_amount,
							)| Record {
								tpep_pickup_datetime: DateTime::from_timestamp_micros(
									*tpep_pickup_datetime,
								)
								.unwrap(),
								passenger_count,
								trip_distance: *trip_distance,
								total_amount: *total_amount,
							},
						),
				);
			} else {
				self.set_next_reader().ok()??;
				return self.next();
			}
		}

		self.batch.pop_front()
	}
}
