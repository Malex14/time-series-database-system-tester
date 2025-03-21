use crate::metrics::MetricsRecorder;
use crate::scenario::scenario_a::{DatabaseConnection, DeviceDatabaseConnection, Record};
use crate::{
	impl_influxdb_connection, impl_mongo_connection, impl_postgres_connection,
	impl_timescaledb_connection,
};
use async_trait::async_trait;
use bson::doc;
use common::do_not_optimize_read;
use futures::{Stream, StreamExt};
use mongodb::options::{TimeseriesGranularity, TimeseriesOptions};
use serde::{Deserialize, Serialize};
use sqlx::{Connection, Postgres};
use std::fmt::Display;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub(super) struct DatabaseConnectionInfluxDB {
	url: String,
	token: String,
	client: reqwest::Client,
}

impl DatabaseConnectionInfluxDB {
	impl_influxdb_connection!();
}

#[async_trait]
impl DatabaseConnection for DatabaseConnectionInfluxDB {
	async fn initialize_schema(&mut self) -> anyhow::Result<()> {
		// InfluxDB does not require schema initialization
		Ok(())
	}

	async fn count_doors_windows_open(&mut self) -> anyhow::Result<()> {
		self.do_flux_query(
			r#"
from(bucket: "bucket")
	  |> range(start: 0)
	  |> filter(fn: (r) => r._measurement == "doors_and_windows" and r.value == true)
	  |> count()
	  |> yield(name: "open_events")
	  			"#,
		)
		.await
	}

	async fn total_energy(&mut self) -> anyhow::Result<()> {
		self.do_flux_query(
			r#"
from(bucket: "bucket")
	  |> range(start: 0)
	  |> filter(fn: (r) => r._measurement == "power")
	  |> sum()
	  |> yield(name: "total_energy")
			"#,
		)
		.await
	}

	async fn average_temperature_per_min(&mut self) -> anyhow::Result<()> {
		self.do_flux_query(
			r#"
from(bucket: "bucket")
	  |> range(start: 0)
	  |> filter(fn: (r) => r._measurement == "temperature")
	  |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
	  |> yield(name: "average_temperature")
			"#,
		)
		.await
	}
}

#[async_trait]
impl<T: Send + Display + 'static> DeviceDatabaseConnection<T> for DatabaseConnectionInfluxDB {
	async fn write_data(
		&mut self,
		data: Box<dyn Stream<Item = Record<T>> + Send + 'static>,
		metrics_recorder: Arc<MetricsRecorder>,
	) -> anyhow::Result<()> {
		let mut pin = Box::into_pin(data);

		while let Some(Record {
			time,
			category,
			id,
			value,
		}) = pin.next().await
		{
			let start = Instant::now();
			let res = self
				.client
				.post(format!(
					"{}/api/v2/write?org=org&bucket=bucket&precision=s",
					self.url
				))
				.header("Authorization", format!("Token {}", self.token))
				.timeout(Duration::from_secs(u64::MAX))
				.body(format!(
					"{},id={} value={} {}",
					category,
					id,
					value,
					time.timestamp()
				))
				.send()
				.await?;
			metrics_recorder.add_latency_metric(start.elapsed());

			Self::check_status(&res).await?;
		}

		Ok(())
	}
}

pub(super) struct DatabaseConnectionTimescaleDB {
	connection: DatabaseConnectionPostgres,
}

impl DatabaseConnectionTimescaleDB {
	impl_timescaledb_connection!();
}

#[async_trait]
impl DatabaseConnection for DatabaseConnectionTimescaleDB {
	async fn initialize_schema(&mut self) -> anyhow::Result<()> {
		self.connection.initialize_schema().await?;

		for table in ["power", "temperature", "doors_and_windows", "lights"] {
			sqlx::query(&format!(
				r#"
SELECT create_hypertable('{table}', by_range('time'));
			"#
			))
			.execute(&mut self.connection.connection)
			.await?;

			sqlx::query(&format!(
				r#"
ALTER TABLE {table} SET (
  timescaledb.compress,
  timescaledb.compress_orderby = 'time',
  timescaledb.compress_segmentby = 'id'
);
			"#
			))
			.execute(&mut self.connection.connection)
			.await?;
		}

		Ok(())
	}

	async fn count_doors_windows_open(&mut self) -> anyhow::Result<()> {
		self.connection.count_doors_windows_open().await
	}

	async fn total_energy(&mut self) -> anyhow::Result<()> {
		self.connection.total_energy().await
	}

	async fn average_temperature_per_min(&mut self) -> anyhow::Result<()> {
		self.connection
			.do_query(
				r#"
SELECT time_bucket('1 minute', time) AS time, AVG(value) AS value
FROM temperature
GROUP BY time;
			"#,
			)
			.await
	}
}

#[async_trait]
impl<T: Send + 'static + sqlx::Type<Postgres>> DeviceDatabaseConnection<T>
	for DatabaseConnectionTimescaleDB
where
	for<'q> T: sqlx::Encode<'q, Postgres>,
{
	async fn write_data(
		&mut self,
		data: Box<dyn Stream<Item = Record<T>> + Send + 'static>,
		metrics_recorder: Arc<MetricsRecorder>,
	) -> anyhow::Result<()> {
		self.connection.write_data(data, metrics_recorder).await
	}
}

pub(super) struct DatabaseConnectionMongoDB {
	client: mongodb::Client,
}

impl DatabaseConnectionMongoDB {
	impl_mongo_connection!();
}

#[async_trait]
impl DatabaseConnection for DatabaseConnectionMongoDB {
	async fn initialize_schema(&mut self) -> anyhow::Result<()> {
		let db = self.client.database("home");

		let ts_opts = TimeseriesOptions::builder()
			.time_field("time")
			.meta_field("id".to_string())
			.granularity(TimeseriesGranularity::Seconds)
			.build();
		for collection in ["power", "temperature", "doors_and_windows", "lights"] {
			db.create_collection(collection)
				.timeseries(ts_opts.clone())
				.await?;
		}

		Ok(())
	}

	async fn count_doors_windows_open(&mut self) -> anyhow::Result<()> {
		let collection = self
			.client
			.database("home")
			.collection::<Record<bool>>("doors_and_windows");

		let pipeline = vec![
			doc! {
				"$match": {
					"value": true
				}
			},
			doc! {
				"$count": "open_events"
			},
		];

		let mut res = collection.aggregate(pipeline).await?;
		while let Some(doc) = res.next().await {
			do_not_optimize_read(&doc?)
		}

		Ok(())
	}

	async fn total_energy(&mut self) -> anyhow::Result<()> {
		let collection = self
			.client
			.database("home")
			.collection::<Record<f64>>("power");

		let pipeline = vec![doc! {
			"$group": {
				"_id": null,
				"total_energy": {
					"$sum": "$value"
				}
			}
		}];

		let mut res = collection.aggregate(pipeline).await?;
		while let Some(doc) = res.next().await {
			do_not_optimize_read(&doc?)
		}

		Ok(())
	}

	async fn average_temperature_per_min(&mut self) -> anyhow::Result<()> {
		let collection = self
			.client
			.database("home")
			.collection::<Record<f64>>("temperature");

		let pipeline = vec![doc! {
			"$group": {
				"_id": {
					"$dateTrunc": {
						"date": "$time",
						"unit": "minute"
					}
				},
				"average_temperature": {
					"$avg": "$value"
				}
			}
		}];

		let mut res = collection.aggregate(pipeline).await?;
		while let Some(doc) = res.next().await {
			do_not_optimize_read(&doc?)
		}

		Ok(())
	}
}

#[derive(Serialize, Deserialize)]
struct MongoRecord<T> {
	time: bson::DateTime,
	id: i64,
	value: T,
}

#[async_trait]
impl<T: Send + Sync + Into<bson::Bson> + Serialize + 'static> DeviceDatabaseConnection<T>
	for DatabaseConnectionMongoDB
{
	async fn write_data(
		&mut self,
		data: Box<dyn Stream<Item = Record<T>> + Send + 'static>,
		metrics_recorder: Arc<MetricsRecorder>,
	) -> anyhow::Result<()> {
		let mut pin = Box::into_pin(data);

		while let Some(record) = pin.next().await {
			let mongo_record = MongoRecord {
				time: record.time.into(),
				id: record.id,
				value: record.value,
			};

			let start = Instant::now();
			self.client
				.database("home")
				.collection(&record.category)
				.insert_one(bson::to_document(&mongo_record)?)
				.await?;
			metrics_recorder.add_latency_metric(start.elapsed());
		}

		Ok(())
	}
}

pub(super) struct DatabaseConnectionPostgres {
	connection: sqlx::PgConnection,
}

impl DatabaseConnectionPostgres {
	impl_postgres_connection!();
}

#[async_trait]
impl DatabaseConnection for DatabaseConnectionPostgres {
	async fn initialize_schema(&mut self) -> anyhow::Result<()> {
		sqlx::query(
			r#"
CREATE TABLE power (
	time TIMESTAMPTZ NOT NULL,
	id INTEGER NOT NULL,
	value FLOAT NOT NULL
);
			"#,
		)
		.execute(&mut self.connection)
		.await?;

		sqlx::query(
			r#"
CREATE TABLE temperature (
	time TIMESTAMPTZ NOT NULL,
	id INTEGER NOT NULL,
	value FLOAT NOT NULL
);
			"#,
		)
		.execute(&mut self.connection)
		.await?;

		sqlx::query(
			r#"
CREATE TABLE doors_and_windows (
	time TIMESTAMPTZ NOT NULL,
	id INTEGER NOT NULL,
	value BOOLEAN NOT NULL
);
			"#,
		)
		.execute(&mut self.connection)
		.await?;

		sqlx::query(
			r#"
CREATE TABLE lights (
	time TIMESTAMPTZ NOT NULL,
	id INTEGER NOT NULL,
	value FLOAT NOT NULL
);
			"#,
		)
		.execute(&mut self.connection)
		.await?;

		Ok(())
	}

	async fn count_doors_windows_open(&mut self) -> anyhow::Result<()> {
		self.do_query(
			r#"
SELECT COUNT(*)
FROM doors_and_windows
WHERE value = TRUE;
			"#,
		)
		.await
	}

	async fn total_energy(&mut self) -> anyhow::Result<()> {
		self.do_query(
			r#"
SELECT SUM(value)
FROM power;
			"#,
		)
		.await
	}

	async fn average_temperature_per_min(&mut self) -> anyhow::Result<()> {
		self.do_query(
			r#"
SELECT date_trunc('minute', time) AS time, AVG(value) AS value
FROM temperature
GROUP BY time;
			"#,
		)
		.await
	}
}

#[async_trait]
impl<T: Send + 'static + sqlx::Type<Postgres>> DeviceDatabaseConnection<T>
	for DatabaseConnectionPostgres
where
	for<'q> T: sqlx::Encode<'q, Postgres>,
{
	async fn write_data(
		&mut self,
		data: Box<dyn Stream<Item = Record<T>> + Send + 'static>,
		metrics_recorder: Arc<MetricsRecorder>,
	) -> anyhow::Result<()> {
		let mut pin = Box::into_pin(data);

		while let Some(Record {
			time,
			category,
			id,
			value,
		}) = pin.next().await
		{
			let start = Instant::now();
			sqlx::query(&format!(
				r#"
INSERT INTO {category} (time, id, value)
VALUES ($1, $2, $3);
					"#,
			))
			.bind(time)
			.bind(id)
			.bind(value)
			.execute(&mut self.connection)
			.await?;

			metrics_recorder.add_latency_metric(start.elapsed());
		}

		Ok(())
	}
}
