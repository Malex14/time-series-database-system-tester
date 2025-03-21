use common::metrics::SystemMetrics;
use log::{debug, info};
use std::fs::OpenOptions;
use std::io::Write;

pub struct MetricsSender {
	out: Box<dyn Write>,
}

impl MetricsSender {
	/// Creates a new MetricsSender with the specified channel
	pub fn new(channel: &str) -> Result<Self, std::io::Error> {
		info!("Initializing MetricsSender");

		info!("Opening channel \"{channel}\"");
		let channel = OpenOptions::new()
			.write(true)
			.create(false)
			.truncate(false)
			.open(channel)?;

		info!("MetricsSender initialized");

		Ok(Self {
			out: Box::new(channel),
		})
	}

	/// Send metrics over chanel device to receiver
	pub fn send(&mut self, metrics: &SystemMetrics) -> anyhow::Result<()> {
		debug!("Converting metrics to json");
		let metrics_string = serde_json::to_string(metrics)?;

		debug!("Sending metrics over serial port");
		self.out.write_all(metrics_string.as_bytes())?;
		self.out.write_all(b"\n")?;

		debug!("Successfully send metrics");
		Ok(())
	}
}
