use crate::scenario::scenario_b::{DatabaseConnection, Record};
use crate::{
	impl_influxdb_connection, impl_mongo_connection, impl_postgres_connection,
	impl_timescaledb_connection,
};
use async_trait::async_trait;
use bson::doc;
use common::do_not_optimize_read;
use futures::StreamExt;
use log::info;
use mongodb::bson;
use mongodb::options::TimeseriesOptions;
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
		// InfluxDB does not require schema initialization
		Ok(())
	}

	async fn import_data(
		&mut self,
		mut data: Box<dyn Iterator<Item = Record> + Send + 'static>,
	) -> anyhow::Result<()> {
		let (tx, mut rx) = channel::<String>(5);

		tokio::spawn(async move {
			let mut buf = String::new();

			loop {
				buf.clear();
				for record in data.by_ref().take(Self::CHUNK_SIZE) {
					writeln!(
						buf,
						"trips passenger_count={},trip_distance={},total_amount={} {}",
						record.passenger_count,
						record.trip_distance,
						record.total_amount,
						record.tpep_pickup_datetime.timestamp(),
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
					"{}/api/v2/write?org=org&bucket=bucket&precision=s",
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

	async fn query_average_fare_price_per_month(&mut self) -> anyhow::Result<()> {
		self.do_flux_query(
			r#"
from(bucket: "bucket")
  |> range(start: 0)
  |> filter(fn: (r) => r["_measurement"] == "trips")
  |> filter(fn: (r) => r["_field"] == "passenger_count")
  |> aggregateWindow(every: 1mo, fn: mean, createEmpty: false)
  |> drop(columns: ["_start", "_stop"])
  |> yield(name: "mean")
			"#,
		)
		.await
	}

	async fn query_passenger_counts(&mut self) -> anyhow::Result<()> {
		self.do_flux_query(
			r#"
from(bucket: "bucket") 
  |> range(start: 2023-01-01T00:00:00Z, stop: 2023-06-30T23:59:59Z)
  |> filter(fn: (r) => r["_measurement"] == "trips")
  |> filter(fn: (r) => r["_field"] == "passenger_count")
  |> drop(columns: ["_start", "_stop"])
  |> yield(name: "passenger_count")
			"#,
		)
		.await
	}

	async fn query_all_fare_distances_higher_than_20(&mut self) -> anyhow::Result<()> {
		self.do_flux_query(
			r#"
from(bucket: "bucket")
  |> range(start: 0)
  |> filter(fn: (r) => r["_measurement"] == "trips")
  |> filter(fn: (r) => r["_field"] == "trip_distance")
  |> filter(fn: (r) => r["_value"] > 20)
  |> drop(columns: ["_start", "_stop"])
  |> yield(name: "trip_distance")
			"#,
		)
		.await
	}

	async fn query_hour_with_most_fares(&mut self) -> anyhow::Result<()> {
		self.do_flux_query(
			r#"
import "date"

from(bucket: "bucket")
  |> range(start: 0)
  |> filter(fn: (r) => r["_measurement"] == "trips")
  |> filter(fn: (r) => r["_field"] == "passenger_count")
  |> aggregateWindow(every: 1h, fn: sum, createEmpty: false)
  |> drop(columns: ["_start", "_stop"])
  |> map(fn: (r) => ({r with hour: date.hour(t: r._time)}))
  |> group(columns: ["hour"], mode: "by")
  |> sum(column: "_value")
  |> group()
  |> sort(columns: ["hour"], desc: true)
  |> limit(n: 1)
  |> yield(name: "count")
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
SELECT create_hypertable('nyc_taxi_trips', by_range('tpep_pickup_datetime'));
			"#,
		)
		.execute(&mut self.connection.connection)
		.await?;

		sqlx::query(
			r#"
ALTER TABLE nyc_taxi_trips SET (
  timescaledb.compress,
  timescaledb.compress_orderby = 'tpep_pickup_datetime'
);
			"#,
		)
		.execute(&mut self.connection.connection)
		.await?;

		Ok(())
	}

	async fn import_data(
		&mut self,
		data: Box<dyn Iterator<Item = Record> + Send + 'static>,
	) -> anyhow::Result<()> {
		self.connection.import_data(data).await
	}

	async fn query_average_fare_price_per_month(&mut self) -> anyhow::Result<()> {
		self.connection
			.do_query(
				r#"
SELECT time_bucket('1 month', tpep_pickup_datetime) as month,
	AVG(total_amount) as average
FROM nyc_taxi_trips
GROUP BY month
ORDER BY month ASC;
			"#,
			)
			.await
	}

	async fn query_passenger_counts(&mut self) -> anyhow::Result<()> {
		self.connection.query_passenger_counts().await
	}

	async fn query_all_fare_distances_higher_than_20(&mut self) -> anyhow::Result<()> {
		self.connection
			.query_all_fare_distances_higher_than_20()
			.await
	}

	async fn query_hour_with_most_fares(&mut self) -> anyhow::Result<()> {
		self.connection
			.do_query(
				r#"
SELECT EXTRACT(HOUR FROM bucket_hour) as hour,
	SUM(bucket_count) as count
FROM (
	SELECT time_bucket('1 hour', tpep_pickup_datetime) as bucket_hour,
		COUNT(*) as bucket_count
	FROM nyc_taxi_trips
	GROUP BY bucket_hour
)
GROUP BY hour
ORDER BY count DESC
LIMIT 1;
			"#,
			)
			.await
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
		let db = self.client.database("nyc_taxi_trips");

		let ts_opts = TimeseriesOptions::builder()
			.time_field("tpep_pickup_datetime")
			.bucket_max_span(Duration::from_secs(60 * 60 * 24 * 7))
			.bucket_rounding(Duration::from_secs(60 * 60 * 24 * 7))
			.build();
		db.create_collection("trips").timeseries(ts_opts).await?;

		Ok(())
	}

	async fn import_data(
		&mut self,
		data: Box<dyn Iterator<Item = Record> + Send + 'static>,
	) -> anyhow::Result<()> {
		let collection = self
			.client
			.database("nyc_taxi_trips")
			.collection::<Record>("trips");

		use iter_chunks::IterChunks;
		let mut chunks = data.chunks(Self::CHUNK_SIZE);
		let start = Instant::now();
		while let Some(chunk) = chunks.next() {
			info!("Inserting chunk of records");
			collection.insert_many(chunk).await?;
			info!("Inserted chunk of records");
		}

		info!("All records inserted. Took {:?}", start.elapsed());

		Ok(())
	}

	async fn query_average_fare_price_per_month(&mut self) -> anyhow::Result<()> {
		let collection = self
			.client
			.database("nyc_taxi_trips")
			.collection::<Record>("trips");

		let pipeline = [
			doc! {
				"$group": {
					"_id": {
						"month": {
							"$dateTrunc": {
								"date": "$tpep_pickup_datetime",
								"unit": "month"
							}
						}
					},
					"average": {
						"$avg": "$total_amount"
					}
				}
			},
			doc! {
				"$sort": {
					"_id.month": 1
				}
			},
			doc! {
				"$project": {
					"_id": 0,
					"average": 1,
					"month": {
						"$dateToString": {
							"date": "$_id.month",
							"format": "%Y.%m"
						}
					}
				}
			},
		];

		let mut res = collection.aggregate(pipeline).await?;
		while let Some(doc) = res.next().await {
			do_not_optimize_read(&doc?)
		}

		Ok(())
	}

	async fn query_passenger_counts(&mut self) -> anyhow::Result<()> {
		let collection = self
			.client
			.database("nyc_taxi_trips")
			.collection::<Record>("trips");

		let from_date = bson::DateTime::parse_rfc3339_str("2023-01-01T00:00:00Z")?;
		let to_date = bson::DateTime::parse_rfc3339_str("2023-06-30T23:59:59Z")?;

		let pipeline = [
			doc! {
				"$match": {
					"tpep_pickup_datetime": {
						"$gte": from_date,
						"$lte": to_date
					}
				}
			},
			doc! {
				"$project": {
					"_id": 0,
					"tpep_pickup_datetime": 0,
					"total_amount": 0,
					"trip_distance": 0
				}
			},
		];

		let mut res = collection.aggregate(pipeline).await?;
		while let Some(doc) = res.next().await {
			do_not_optimize_read(&doc?)
		}

		Ok(())
	}

	async fn query_all_fare_distances_higher_than_20(&mut self) -> anyhow::Result<()> {
		let collection = self
			.client
			.database("nyc_taxi_trips")
			.collection::<Record>("trips");

		let mut res = collection
			.find(doc! {
				"trip_distance": {
					"$gt": 20
				}
			})
			.await?;
		while let Some(doc) = res.next().await {
			do_not_optimize_read(&doc?)
		}

		Ok(())
	}

	async fn query_hour_with_most_fares(&mut self) -> anyhow::Result<()> {
		let collection = self
			.client
			.database("nyc_taxi_trips")
			.collection::<Record>("trips");

		let pipeline = [
			doc! {
				"$group": {
					"_id": {
						"hour": {
							"$hour": {
								"date": "$tpep_pickup_datetime"
							}
						}
					},
					"group_count": {
						"$sum": "$passenger_count"
					}
				}
			},
			doc! {
				"$project": {
					"_id": 0,
					"hour": "$_id.hour",
					"group_count": 1
				}
			},
			doc! {
				"$sort": {
					"hour": -1
				}
			},
			doc! {
				"$limit": 1
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
		sqlx::query(
			r#"
CREATE TABLE nyc_taxi_trips (
	tpep_pickup_datetime TIMESTAMPTZ NOT NULL,
	passenger_count FLOAT NOT NULL,
	trip_distance FLOAT NOT NULL,
	total_amount FLOAT NOT NULL
);
			"#,
		)
		.execute(&mut self.connection)
		.await?;

		Ok(())
	}

	async fn import_data(
		&mut self,
		mut data: Box<dyn Iterator<Item = Record> + Send + 'static>,
	) -> anyhow::Result<()> {
		let (tx, mut rx) = channel::<String>(5);
		tokio::spawn(async move {
			let mut buf = String::new();

			loop {
				buf.clear();
				for record in data.by_ref().take(Self::CHUNK_SIZE) {
					writeln!(
						buf,
						"{};{};{};{}",
						record.tpep_pickup_datetime,
						record.passenger_count,
						record.trip_distance,
						record.total_amount,
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
					r#"COPY nyc_taxi_trips 
					(tpep_pickup_datetime, passenger_count, trip_distance, total_amount)
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

	async fn query_average_fare_price_per_month(&mut self) -> anyhow::Result<()> {
		self.do_query(
			r#"
SELECT AVG(total_amount)
FROM nyc_taxi_trips
GROUP BY date_trunc('month', tpep_pickup_datetime);
			"#,
		)
		.await
	}

	async fn query_passenger_counts(&mut self) -> anyhow::Result<()> {
		self.do_query(
			r#"
SELECT tpep_pickup_datetime,
	passenger_count
FROM nyc_taxi_trips
WHERE tpep_pickup_datetime BETWEEN '2023-01-01T00:00:00Z' AND '2023-06-30T23:59:59Z';
			"#,
		)
		.await
	}

	async fn query_all_fare_distances_higher_than_20(&mut self) -> anyhow::Result<()> {
		self.do_query(
			r#"
SELECT *
FROM nyc_taxi_trips
WHERE trip_distance > 20;
			"#,
		)
		.await
	}

	async fn query_hour_with_most_fares(&mut self) -> anyhow::Result<()> {
		self.do_query(
			r#"
SELECT EXTRACT(HOUR FROM tpep_pickup_datetime) as hour,
	COUNT(*) as count
FROM nyc_taxi_trips
GROUP BY hour
ORDER BY count DESC
LIMIT 1;
			"#,
		)
		.await
	}
}
