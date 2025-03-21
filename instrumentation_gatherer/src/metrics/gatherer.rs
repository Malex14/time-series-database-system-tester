use crate::metrics::sender::MetricsSender;
use common::metrics::SystemMetrics;
use log::{debug, error, info};
use std::any::TypeId;
use std::iter::Sum;
use std::ops::Div;
use std::thread;
use std::time::Duration;
use sysinfo::{CpuRefreshKind, DiskRefreshKind, Disks, MemoryRefreshKind, Networks, System};

pub struct MetricsGatherer {
	sender: MetricsSender,
	rate: u64,
	system: System,
	disks: Disks,
	last_disk_reads: Vec<u64>,
	last_disk_writes: Vec<u64>,
	networks: Networks,
	last_network_ins: Vec<u64>,
	last_network_outs: Vec<u64>,
	avg_ptr: usize,
}

impl MetricsGatherer {
	/// Creates a new MetricsGatherer that uses the specified sender to
	/// send the metrics. Additionally, a rate in Hz at which the metrics
	/// are collected and sent has to be provided.
	pub fn new(sender: MetricsSender, rate: u64) -> Self {
		info!("Initializing MetricsGatherer");

		info!("Getting system info");
		let mut system = System::new_all();
		system.refresh_all();
		let disks = Disks::new_with_refreshed_list();
		let networks = Networks::new_with_refreshed_list();

		info!("MetricsGatherer initialized");
		Self {
			sender,
			rate,
			system,
			disks,
			last_disk_writes: vec![0; rate as usize],
			last_disk_reads: vec![0; rate as usize],
			networks,
			last_network_ins: vec![0; rate as usize],
			last_network_outs: vec![0; rate as usize],
			avg_ptr: 0,
		}
	}

	/// Start the collection loop
	pub fn run(&mut self) -> ! {
		info!("Starting main loop");

		let rate = Duration::from_millis(1000 / self.rate);

		loop {
			let metrics = self.get_metrics();
			match self.sender.send(&metrics) {
				Ok(()) => {}
				Err(error) => {
					error!("An error occurred when sending metrics: {error:?}");
				}
			}

			thread::sleep(rate)
		}
	}

	fn get_metrics(&mut self) -> SystemMetrics {
		debug!("Getting system info");
		self.system
			.refresh_cpu_specifics(CpuRefreshKind::nothing().with_cpu_usage());
		self.system
			.refresh_memory_specifics(MemoryRefreshKind::nothing().with_ram());

		let cpu_usage: f64 = self
			.system
			.cpus()
			.iter()
			.map(|c| c.cpu_usage() as f64)
			.sum::<f64>()
			/ self.system.cpus().len() as f64;

		let ram_used: u64 = self.system.used_memory();
		let ram_total: u64 = self.system.total_memory();

		debug!("Getting disk info");
		self.disks.refresh_specifics(
			true,
			DiskRefreshKind::nothing().with_io_usage().with_storage(),
		);
		let mut disk_total: u64 = 0;
		let mut disk_used: u64 = 0;
		let mut disk_read: u64 = 0;
		let mut disk_write: u64 = 0;

		for disk in &self.disks {
			// Only look as real disks (skip tempfs, ...)
			if disk.name().to_string_lossy().starts_with("/dev/vd") {
				disk_total += disk.total_space();
				disk_used += disk.total_space() - disk.available_space();

				disk_read += disk.usage().read_bytes;
				disk_write += disk.usage().written_bytes;
			}
		}
		// Calculate moving average of disk i/o
		Self::calculate_moving_avg(&mut disk_read, self.avg_ptr, &mut self.last_disk_reads);
		Self::calculate_moving_avg(&mut disk_write, self.avg_ptr, &mut self.last_disk_writes);

		let mut network_in: u64 = 0;
		let mut network_out: u64 = 0;

		self.networks.refresh(true);
		for (interface_name, data) in &self.networks {
			// Skip localhost
			if interface_name == "lo" {
				continue;
			}

			network_in += data.received();
			network_out = data.transmitted();
		}
		// Calculate moving average of network i/o
		Self::calculate_moving_avg(&mut network_in, self.avg_ptr, &mut self.last_network_ins);
		Self::calculate_moving_avg(&mut network_out, self.avg_ptr, &mut self.last_network_outs);

		self.avg_ptr += 1;
		self.avg_ptr %= self.rate as usize;

		SystemMetrics {
			cpu_usage,
			ram_used,
			ram_total,
			disk_used,
			disk_total,
			disk_read,
			disk_write,
			network_in,
			network_out,
		}
	}

	fn calculate_moving_avg<
		'a,
		T: Copy + Sum<&'a T> + Div<T, Output = T> + TryFrom<usize> + 'static,
	>(
		new_value: &mut T,
		position: usize,
		last_values: &'a mut [T],
	) {
		last_values[position] = *new_value;
		*new_value = last_values.iter().sum::<T>()
			/ last_values.len().try_into().unwrap_or_else(|_| {
				panic!("Conversion from usize to {:?} failed", TypeId::of::<T>())
			});
	}
}
