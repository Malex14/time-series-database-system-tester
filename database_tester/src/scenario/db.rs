#[macro_export]
macro_rules! impl_influxdb_connection {
	() => {
		pub(super) const CHUNK_SIZE: usize = 500_000;

		pub(super) async fn new(url: &str, token: &str) -> anyhow::Result<Self> {
			let client = reqwest::ClientBuilder::new().gzip(true).build()?;

			if client.get(format!("{}/ping", url)).send().await?.status() != 204 {
				return Err(anyhow::anyhow!("InfluxDB not reachable"));
			}

			Ok(Self {
				url: url.to_string(),
				token: token.to_string(),
				client,
			})
		}

		pub(super) async fn do_flux_query(&mut self, query: &str) -> anyhow::Result<()> {
			let res = self
				.client
				.post(format!("{}/api/v2/query?org=org", self.url))
				.header("Authorization", format!("Token {}", self.token))
				.header("Content-Type", "application/vnd.flux")
				.body(query.to_string())
				.send()
				.await?;

			Self::check_status(&res).await?;

			common::do_not_optimize_read(&res.bytes_stream().count().await);
			Ok(())
		}

		pub(super) async fn check_status(res: &reqwest::Response) -> anyhow::Result<()> {
			if res.status().as_u16() < 200 || res.status().as_u16() >= 300 {
				return Err(anyhow::anyhow!(
					"InfluxDB returned status code {}",
					res.status(),
				));
			}
			Ok(())
		}
	};
}

#[macro_export]
macro_rules! impl_timescaledb_connection {
	() => {
		pub(super) async fn new(url: &str) -> anyhow::Result<Self> {
			Ok(Self {
				connection: DatabaseConnectionPostgres::new(url).await?,
			})
		}
	};
}

#[macro_export]
macro_rules! impl_mongo_connection {
	() => {
		const CHUNK_SIZE: usize = 5_000_000;

		pub(super) async fn new(url: &str) -> anyhow::Result<Self> {
			use mongodb::options::{ClientOptions, ServerApi, ServerApiVersion};
			use mongodb::Client;

			let mut client_options = ClientOptions::parse(url).await?;
			let server_api = ServerApi::builder().version(ServerApiVersion::V1).build();
			client_options.server_api = Some(server_api);

			let client = Client::with_options(client_options)?;

			Ok(Self { client })
		}
	};
}

#[macro_export]
macro_rules! impl_postgres_connection {
	() => {
		const CHUNK_SIZE: usize = 5_000_000;

		pub(super) async fn new(url: &str) -> anyhow::Result<Self> {
			let connection = sqlx::PgConnection::connect(url).await?;
			Ok(Self { connection })
		}

		async fn do_query(&mut self, query: &str) -> anyhow::Result<()> {
			sqlx::query(query)
				.fetch(&mut self.connection)
				.for_each(|e| async move {
					do_not_optimize_read(&e);
				})
				.await;

			Ok(())
		}
	};
}
