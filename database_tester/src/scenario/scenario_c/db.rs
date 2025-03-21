use crate::scenario::scenario_c::generator::SystemMetrics;
use crate::scenario::scenario_c::DatabaseConnection;
use crate::{
	impl_influxdb_connection, impl_mongo_connection, impl_postgres_connection,
	impl_timescaledb_connection,
};
use anyhow::anyhow;
use async_trait::async_trait;
use bson::doc;
use chrono::{DateTime, Utc};
use common::do_not_optimize_read;
use futures::StreamExt;
use log::info;
use mongodb::options::{TimeseriesGranularity, TimeseriesOptions};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::Connection;
use std::fmt::Write;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::channel;

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
		info!("Setting retention policy");
		let res = self
			.client
			.get(format!("{}/api/v2/buckets?name=bucket", self.url))
			.header("Authorization", format!("Token {}", self.token))
			.send()
			.await?;
		if res.status() != 200 {
			return Err(anyhow!("InfluxDB returned status code {}", res.status()));
		}
		let json = res.text().await?;
		let json = serde_json::from_str::<serde_json::Value>(&json)?;
		let bucket_id = json
			.get("buckets")
			.ok_or(anyhow!("No buckets found"))?
			.as_array()
			.ok_or(anyhow!("Buckets is not an array"))?[0]
			.get("id")
			.ok_or(anyhow!("No id found"))?
			.as_str()
			.ok_or(anyhow!("Id is not a string"))?;

		let res = self
			.client
			.patch(format!("{}/api/v2/buckets/{}", self.url, bucket_id))
			.header("Authorization", format!("Token {}", self.token))
			.json(&json!(
				{
					"retentionRules": [
						{
							"type": "expire",
							"everySeconds": 0,
							"shardGroupDurationSeconds": 60 * 30,
						}
					]
				}
			))
			.send()
			.await?;
		if res.status() != 200 {
			return Err(anyhow!("InfluxDB returned status code {}", res.status()));
		}

		Ok(())
	}

	async fn import_data(
		&mut self,
		data: Box<dyn Iterator<Item = Vec<(DateTime<Utc>, SystemMetrics)>> + Send + 'static>,
	) -> anyhow::Result<()> {
		let (tx, mut rx) = channel::<String>(5);

		tokio::spawn(async move {
			let mut buf = String::new();
			let mut data = data.flatten();

			loop {
				buf.clear();
				for (time, metrics) in data.by_ref().take(Self::CHUNK_SIZE) {
					writeln!(
						buf,
						"system_metrics,system_id={} cpu_load={},memory_usage={} {}",
						metrics.id,
						metrics.cpu,
						metrics.memory,
						time.timestamp_millis(),
					)
					.unwrap();
				}
				if buf.is_empty() {
					info!("No more records to send");
					break;
				}
				info!("Sending chunk of records");
				tx.send(buf.clone()).await.unwrap();
			}
		});

		while let Some(buf) = rx.recv().await {
			info!("Inserting chunk of records");

			let res = self
				.client
				.post(format!(
					"{}/api/v2/write?org=org&bucket=bucket&precision=ms",
					self.url
				))
				.header("Authorization", format!("Token {}", self.token))
				.timeout(Duration::from_secs(u64::MAX))
				.body(buf)
				.send()
				.await?;

			Self::check_status(&res).await?;

			info!("Inserted chunk of records");
		}

		Ok(())
	}

	async fn query_combined_cpu_load_per_minute(&mut self) -> anyhow::Result<()> {
		self.do_flux_query(
			r#"
from(bucket: "bucket")
	|> range(start: 0)
	|> filter(fn: (r) => r._measurement == "system_metrics")
	|> filter(fn: (r) => r._field == "cpu_load")
	|> group()
	|> aggregateWindow(every: 1m, fn: mean)
	|> yield(name: "mean")
		"#,
		)
		.await
	}

	async fn query_all_metrics(&mut self) -> anyhow::Result<()> {
		self.do_flux_query(
			r#"
from(bucket: "bucket")
	|> range(start: 2025-01-01T05:00:00Z, stop: 2025-01-01T05:59:59Z)
	|> filter(fn: (r) => r._measurement == "system_metrics")
		"#,
		)
		.await
	}

	async fn query_all_instances_with_cpu_load_higher_than_95_and_memory_usage_higher_than_2350000(
		&mut self,
	) -> anyhow::Result<()> {
		self.do_flux_query(
			r#"
from(bucket: "bucket")
    |> range(start: 0)
    |> filter(fn: (r) => r._measurement == "system_metrics" and ((r._field == "cpu_load" and r._value > 0.95) or (r._field == "memory_usage" and r._value > 2350000)))
    |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
    |> filter(fn: (r) => r.cpu_load > 0.95 and r.memory_usage > 2350000)
    |> first(column: "system_id")
    |> group(columns: ["_result"])
    |> keep(columns: ["system_id"])
    |> yield(name: "system_id")
		"#,
		)
		.await
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

		sqlx::query(
			r#"
SELECT create_hypertable('system_metrics', by_range('time'));
			"#,
		)
		.execute(&mut self.connection.connection)
		.await?;

		sqlx::query(
			r#"
ALTER TABLE system_metrics SET (
  timescaledb.compress,
  timescaledb.compress_orderby = 'time',
  timescaledb.compress_segmentby = 'system_id'
);
			"#,
		)
		.execute(&mut self.connection.connection)
		.await?;

		Ok(())
	}

	async fn import_data(
		&mut self,
		data: Box<dyn Iterator<Item = Vec<(DateTime<Utc>, SystemMetrics)>> + Send + 'static>,
	) -> anyhow::Result<()> {
		self.connection.import_data(data).await
	}

	async fn query_combined_cpu_load_per_minute(&mut self) -> anyhow::Result<()> {
		self.connection
			.do_query(
				r#"
SELECT time_bucket('1 minute', time) AS time, AVG(cpu) AS avg_cpu
FROM system_metrics
GROUP BY time
ORDER BY time ASC;
		"#,
			)
			.await
	}

	async fn query_all_metrics(&mut self) -> anyhow::Result<()> {
		self.connection.query_all_metrics().await
	}

	async fn query_all_instances_with_cpu_load_higher_than_95_and_memory_usage_higher_than_2350000(
		&mut self,
	) -> anyhow::Result<()> {
		self.connection
			.query_all_instances_with_cpu_load_higher_than_95_and_memory_usage_higher_than_2350000()
			.await
	}
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct MongoSystemMetrics {
	time: bson::DateTime,
	system_id: i64,
	cpu: f64,
	mem: i64,
}

impl From<(DateTime<Utc>, SystemMetrics)> for MongoSystemMetrics {
	fn from((time, metric): (DateTime<Utc>, SystemMetrics)) -> Self {
		Self {
			time: bson::DateTime::from(time),
			system_id: metric.id as i64,
			cpu: metric.cpu,
			mem: metric.memory as i64,
		}
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
		let db = self.client.database("system_metrics");

		let ts_opts = TimeseriesOptions::builder()
			.time_field("time")
			.meta_field("system_id".to_string())
			.granularity(TimeseriesGranularity::Seconds)
			.build();
		db.create_collection("metrics").timeseries(ts_opts).await?;

		Ok(())
	}

	async fn import_data(
		&mut self,
		data: Box<dyn Iterator<Item = Vec<(DateTime<Utc>, SystemMetrics)>> + Send + 'static>,
	) -> anyhow::Result<()> {
		let collection = self
			.client
			.database("system_metrics")
			.collection::<MongoSystemMetrics>("metrics");

		use iter_chunks::IterChunks;
		let mut chunks = data.flatten().chunks(Self::CHUNK_SIZE);

		let start = Instant::now();
		while let Some(chunk) = chunks.next() {
			info!("Inserting chunk of records");
			collection
				.insert_many(chunk.map(MongoSystemMetrics::from))
				.await?;
			info!("Inserted chunk of records");
		}

		info!("All records inserted. Took {:?}", start.elapsed());

		Ok(())
	}

	async fn query_combined_cpu_load_per_minute(&mut self) -> anyhow::Result<()> {
		let collection = self
			.client
			.database("system_metrics")
			.collection::<MongoSystemMetrics>("metrics");

		let pipeline = [
			doc! {
				"$group": {
					"_id": {
						"minute": {
							"$minute": {
								"date": "$time",
							}
						}
					},
					"cpu": {
						"$avg": "$cpu"
					}
				}
			},
			doc! {
				"$project": {
					"_id": 0,
					"cpu": 1,
					"time": "$_id"
				}
			},
			doc! {
				"$sort": {
					"time": 1
				}
			},
		];

		let mut res = collection.aggregate(pipeline).await?;
		while let Some(doc) = res.next().await {
			do_not_optimize_read(&doc?)
		}

		Ok(())
	}

	async fn query_all_metrics(&mut self) -> anyhow::Result<()> {
		let collection = self
			.client
			.database("system_metrics")
			.collection::<MongoSystemMetrics>("metrics");

		let from_time = bson::DateTime::parse_rfc3339_str("2025-01-01T05:00:00Z")?;
		let to_time = bson::DateTime::parse_rfc3339_str("2025-01-01T05:59:59Z")?;

		let mut res = collection
			.find(doc! {
				"time": {
					"$gte": from_time,
					"$lte": to_time
				}
			})
			.await?;
		while let Some(doc) = res.next().await {
			do_not_optimize_read(&doc?)
		}

		Ok(())
	}

	async fn query_all_instances_with_cpu_load_higher_than_95_and_memory_usage_higher_than_2350000(
		&mut self,
	) -> anyhow::Result<()> {
		let collection = self
			.client
			.database("system_metrics")
			.collection::<MongoSystemMetrics>("metrics");

		let pipeline = [
			doc! {
				"$match": {
					"$and": [
						{ "cpu": { "$gt": 0.95 } },
						{ "mem": { "$gt": 2350000 } }
					]
				}
			},
			doc! {
				"$group": {
					"_id": "$system_id"
				}
			},
			doc! {
				"$project": {
					"_id": 0,
					"system_id": "$_id"
				}
			},
			doc! {
				"$sort": {
					"system_id": 1
				}
			},
		];

		let mut res = collection.aggregate(pipeline).await?;
		while let Some(doc) = res.next().await {
			do_not_optimize_read(&doc?)
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
		self.do_query(
			r#"
CREATE TABLE system_metrics (
	time TIMESTAMPTZ NOT NULL,
	system_id INTEGER NOT NULL,
	cpu FLOAT NOT NULL,
	memory INTEGER NOT NULL
);
		"#,
		)
		.await?;

		Ok(())
	}

	async fn import_data(
		&mut self,
		data: Box<dyn Iterator<Item = Vec<(DateTime<Utc>, SystemMetrics)>> + Send + 'static>,
	) -> anyhow::Result<()> {
		let (tx, mut rx) = channel::<String>(5);
		tokio::spawn(async move {
			let mut buf = String::new();
			let mut data = data.flatten();

			loop {
				buf.clear();
				for (time, metric) in data.by_ref().take(Self::CHUNK_SIZE) {
					writeln!(
						buf,
						"{};{};{};{}",
						time, metric.id, metric.cpu, metric.memory
					)
					.unwrap();
				}
				if buf.is_empty() {
					info!("No more records to send");
					break;
				}
				info!("Sending chunk of records");
				tx.send(buf.clone()).await.unwrap();
			}
		});

		while let Some(buf) = rx.recv().await {
			info!("Inserting chunk of records");
			let mut copy = self
				.connection
				.copy_in_raw(
					r#"COPY system_metrics
					(time, system_id, cpu, memory)
					FROM STDIN (
						FORMAT TEXT,
						DELIMITER ';'
					);
				"#,
				)
				.await?;
			copy.read_from(&mut buf.as_bytes()).await?;
			copy.finish().await?;
			info!("Inserted chunk of records");
		}

		Ok(())
	}

	async fn query_combined_cpu_load_per_minute(&mut self) -> anyhow::Result<()> {
		self.do_query(
			r#"
SELECT time, AVG(cpu) AS avg_cpu
FROM system_metrics
GROUP BY time
ORDER BY time;
		"#,
		)
		.await
	}

	async fn query_all_metrics(&mut self) -> anyhow::Result<()> {
		self.do_query(
			r#"
SELECT *
FROM system_metrics
WHERE time BETWEEN '2025-01-01T05:00:00Z' AND '2025-01-01T05:59:59Z'
ORDER BY time;
		"#,
		)
		.await
	}

	async fn query_all_instances_with_cpu_load_higher_than_95_and_memory_usage_higher_than_2350000(
		&mut self,
	) -> anyhow::Result<()> {
		self.do_query(
			r#"
SELECT DISTINCT system_id
FROM system_metrics
WHERE cpu > 0.95 AND memory > 2350000;
		"#,
		)
		.await
	}
}
