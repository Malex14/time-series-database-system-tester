use crate::metrics::energy::EnergyMetricsGatherer;
use crate::metrics::receiver::MetricsReceiver;
use common::metrics::{EnergyMetrics, SystemMetrics};
use csv::WriterBuilder;
use log::{debug, info, warn};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

mod energy;
mod receiver;

pub struct MetricsRecorder {
	out_path: PathBuf,
	system_metrics_receiver: Arc<RwLock<Option<MetricsReceiver>>>,
	energy_metrics_gatherer: EnergyMetricsGatherer,
	inner: RwLock<HashMap<String, MetricsRecorderInner>>,
	active_measurement: RwLock<Option<String>>,
}

#[derive(Default)]
struct MetricsRecorderInner {
	system_metrics: Vec<(SystemTime, SystemMetrics)>,
	energy_metrics: Vec<(SystemTime, EnergyMetrics)>,
	latency_metrics: Vec<(SystemTime, Duration)>,
}

pub struct SystemMetricsReceiver {
	cb: Box<dyn Fn()>,
}

impl Drop for SystemMetricsReceiver {
	fn drop(&mut self) {
		(self.cb)()
	}
}

impl MetricsRecorder {
	pub fn new(
		out_path: impl AsRef<Path>,
		energy_gather_interval: Duration,
	) -> anyhow::Result<Arc<Self>> {
		let s = Arc::new(Self {
			out_path: out_path.as_ref().to_path_buf(),
			system_metrics_receiver: Default::default(),
			energy_metrics_gatherer: EnergyMetricsGatherer::new(energy_gather_interval),
			inner: Default::default(),
			active_measurement: Default::default(),
		});

		{
			let s_clone = s.clone();
			s.energy_metrics_gatherer
				.set_callback(move |metrics| s_clone.add_energy_metric(metrics));
		}

		Ok(s)
	}

	pub fn with_system_metrics_receiver_on_channel(
		self: &Arc<Self>,
		channel: &str,
	) -> anyhow::Result<SystemMetricsReceiver> {
		let mut rec = MetricsReceiver::new(channel);
		let self_clone = self.clone();
		rec.set_callback(move |metrics| self_clone.vm_metrics_received(metrics));
		rec.listen()?;
		*self
			.system_metrics_receiver
			.write()
			.expect("RwLock is poisoned") = Some(rec);

		let self_clone = self.clone();
		Ok(SystemMetricsReceiver {
			cb: Box::new(move || {
				warn!("Dropping SystemMetricsReceiver");
				*self_clone
					.system_metrics_receiver
					.write()
					.expect("RwLock is poisoned") = None;
			}),
		})
	}

	fn vm_metrics_received(&self, metrics: SystemMetrics) {
		debug!("SystemMetrics received: {metrics:?}");
		if let Some(name) = &*self.active_measurement.read().expect("RwLock is poisoned") {
			let time = SystemTime::now();
			let mut inner = self.inner.write().expect("RwLock is poisoned");
			let measurement = inner.get_mut(name).expect("Measurement not found");
			measurement.system_metrics.push((time, metrics));
		}
	}

	fn add_energy_metric(&self, metrics: EnergyMetrics) {
		debug!("Energy metric received: {metrics:?}");
		if let Some(name) = &*self.active_measurement.read().expect("RwLock is poisoned") {
			let time = SystemTime::now();
			let mut inner = self.inner.write().expect("RwLock is poisoned");
			let measurement = inner.get_mut(name).expect("Measurement not found");
			measurement.energy_metrics.push((time, metrics));
		}
	}

	pub fn add_latency_metric(&self, latency: Duration) {
		debug!("Latency metric received: {latency:?}");
		if let Some(name) = &*self.active_measurement.read().expect("RwLock is poisoned") {
			let time = SystemTime::now();
			let mut inner = self.inner.write().expect("RwLock is poisoned");
			let measurement = inner.get_mut(name).expect("Measurement not found");
			measurement.latency_metrics.push((time, latency));
		}
	}

	pub fn start_measurement(&self, name: &str) {
		info!("Starting measurement: {name}");
		let mut lock = self.active_measurement.write().expect("RwLock is poisoned");

		self.inner
			.write()
			.expect("RwLock is poisoned")
			.entry(name.to_string())
			.or_default();

		lock.replace(name.to_string());
	}

	pub fn stop_measurement(&self) {
		let Some(name) = self
			.active_measurement
			.write()
			.expect("RwLock is poisoned")
			.take()
		else {
			warn!("No measurement to stop");
			return;
		};

		info!("Stopping measurement: {}", name);
	}

	pub fn save_measurement(&self, name: &str) -> anyhow::Result<()> {
		let Some(measurements) = self.inner.write().expect("RwLock is poisoned").remove(name)
		else {
			warn!("No measurement to save");
			return Ok(());
		};
		info!("Saving measurement: {}", name);

		let path = self.create_measurement_directory_if_not_exists(name)?;

		self.export_system_metrics(&measurements.system_metrics, &path)?;
		self.export_energy_metrics(&measurements.energy_metrics, &path)?;
		info!("latencies: {:?}", &measurements.latency_metrics);
		self.export_latency_metrics(&measurements.latency_metrics, &path)?;

		Ok(())
	}

	fn create_measurement_directory_if_not_exists(
		&self,
		measurement_name: &str,
	) -> anyhow::Result<PathBuf> {
		let path = self.out_path.join(measurement_name);
		if !path.exists() {
			info!("Creating directory: {path:?}");
			std::fs::create_dir_all(&path)?;
		}
		Ok(path)
	}

	fn export_system_metrics(
		&self,
		metrics: &[(SystemTime, SystemMetrics)],
		path: &Path,
	) -> anyhow::Result<()> {
		info!("Exporting system metrics");

		info!("Exporting raw metrics");
		let mut writer = WriterBuilder::new()
			.delimiter(b';')
			.from_path(path.join("system_metrics.csv"))?;

		writer.write_record([
			"timestamp",
			"cpu_usage",
			"ram_used",
			"ram_total",
			"disk_used",
			"disk_total",
			"disk_read",
			"disk_write",
			"network_in",
			"network_out",
		])?;
		for (time, metrics) in metrics {
			writer.write_record(&[
				time.duration_since(SystemTime::UNIX_EPOCH)?
					.as_secs()
					.to_string(),
				metrics.cpu_usage.to_string(),
				metrics.ram_used.to_string(),
				metrics.ram_total.to_string(),
				metrics.disk_used.to_string(),
				metrics.disk_total.to_string(),
				metrics.disk_read.to_string(),
				metrics.disk_write.to_string(),
				metrics.network_in.to_string(),
				metrics.network_out.to_string(),
			])?;
		}
		writer.flush()?;

		info!("Exporting aggregated metrics");

		let mut writer = WriterBuilder::new()
			.delimiter(b';')
			.from_path(path.join("system_metrics_aggregated.csv"))?;
		writer.write_record([
			"metric", "mean", "q1", "median", "q3", "std_dev", "min", "max",
		])?;

		let mut stats: HashMap<String, Vec<f64>> = HashMap::new();
		for (_, metrics) in metrics {
			stats
				.entry("cpu_usage".to_string())
				.or_default()
				.push(metrics.cpu_usage);
			stats
				.entry("ram_used".to_string())
				.or_default()
				.push(metrics.ram_used as f64);
			stats
				.entry("ram_total".to_string())
				.or_default()
				.push(metrics.ram_total as f64);
			stats
				.entry("disk_used".to_string())
				.or_default()
				.push(metrics.disk_used as f64);
			stats
				.entry("disk_total".to_string())
				.or_default()
				.push(metrics.disk_total as f64);
			stats
				.entry("disk_read".to_string())
				.or_default()
				.push(metrics.disk_read as f64);
			stats
				.entry("disk_write".to_string())
				.or_default()
				.push(metrics.disk_write as f64);
			stats
				.entry("network_in".to_string())
				.or_default()
				.push(metrics.network_in as f64);
			stats
				.entry("network_out".to_string())
				.or_default()
				.push(metrics.network_out as f64);
		}

		for (metric_name, values) in stats {
			let stats = Stats::get_stats(&values);
			writer.write_record(&[
				metric_name,
				stats.mean.to_string(),
				stats.q1.to_string(),
				stats.median.to_string(),
				stats.q3.to_string(),
				stats.std_dev.to_string(),
				stats.min.to_string(),
				stats.max.to_string(),
			])?;
		}
		writer.flush()?;

		Ok(())
	}

	fn export_energy_metrics(
		&self,
		metrics: &[(SystemTime, EnergyMetrics)],
		path: &Path,
	) -> anyhow::Result<()> {
		info!("Exporting raw energy metrics");
		let mut writer = WriterBuilder::new()
			.delimiter(b';')
			.from_path(path.join("energy_metrics.csv"))?;

		writer.write_record(["timestamp", "power_usage"])?;
		for (time, metrics) in metrics {
			writer.write_record(&[
				time.duration_since(SystemTime::UNIX_EPOCH)?
					.as_secs()
					.to_string(),
				metrics.power_usage.to_string(),
			])?;
		}
		writer.flush()?;

		info!("Exporting aggregated energy metrics");
		let mut writer = WriterBuilder::new()
			.delimiter(b';')
			.from_path(path.join("energy_metrics_aggregated.csv"))?;
		writer.write_record(["mean", "q1", "median", "q3", "std_dev", "min", "max", "sum"])?;

		let values: Vec<f64> = metrics.iter().map(|(_, m)| m.power_usage).collect();
		let stats = Stats::get_stats(&values);
		writer.write_record(&[
			stats.mean.to_string(),
			stats.q1.to_string(),
			stats.median.to_string(),
			stats.q3.to_string(),
			stats.std_dev.to_string(),
			stats.min.to_string(),
			stats.max.to_string(),
			values.iter().sum::<f64>().to_string(),
		])?;
		writer.flush()?;

		Ok(())
	}

	fn export_latency_metrics(
		&self,
		metrics: &[(SystemTime, Duration)],
		path: &Path,
	) -> anyhow::Result<()> {
		info!("Exporting raw latency metrics");
		let mut writer = WriterBuilder::new()
			.delimiter(b';')
			.from_path(path.join("latency_metrics.csv"))?;
		writer.write_record(["timestamp", "latency"])?;
		for (time, latency) in metrics {
			writer.write_record(&[
				time.duration_since(SystemTime::UNIX_EPOCH)?
					.as_secs()
					.to_string(),
				latency.as_secs_f64().to_string(),
			])?;
		}

		info!("Exporting aggregated latency metrics");
		let mut writer = WriterBuilder::new()
			.delimiter(b';')
			.from_path(path.join("latency_metrics_aggregated.csv"))?;
		writer.write_record(["mean", "q1", "median", "q3", "std_dev", "min", "max"])?;

		let values: Vec<f64> = metrics.iter().map(|(_, m)| m.as_secs_f64()).collect();
		let stats = Stats::get_stats(&values);
		writer.write_record(&[
			stats.mean.to_string(),
			stats.q1.to_string(),
			stats.median.to_string(),
			stats.q3.to_string(),
			stats.std_dev.to_string(),
			stats.min.to_string(),
			stats.max.to_string(),
		])?;

		Ok(())
	}
}

struct Stats {
	mean: f64,
	q1: f64,
	median: f64,
	q3: f64,
	std_dev: f64,
	min: f64,
	max: f64,
}

impl Stats {
	fn get_stats(items: &[f64]) -> Self {
		let stats = stats::OnlineStats::from_slice(items);
		let mean = stats.mean();
		let (q1, median, q3) =
			stats::quartiles(items.iter().copied()).unwrap_or((f64::NAN, f64::NAN, f64::NAN));
		let std_dev = stats.stddev();
		let min = items.iter().fold(f64::NAN, |acc, x: &f64| acc.min(*x));
		let max = items.iter().fold(f64::NAN, |acc, x: &f64| acc.max(*x));

		Self {
			mean,
			q1,
			median,
			q3,
			std_dev,
			min,
			max,
		}
	}
}
