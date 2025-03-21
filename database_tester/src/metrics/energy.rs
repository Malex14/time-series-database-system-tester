use common::metrics::EnergyMetrics;
use log::warn;
use scaphandre::sensors::Sensor;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::time::sleep;

type Callback = Arc<RwLock<Option<Box<dyn Fn(EnergyMetrics) + Send + Sync>>>>;

pub struct EnergyMetricsGatherer {
	loop_handle: Option<tokio::task::JoinHandle<()>>,
	callback: Callback,
}

impl EnergyMetricsGatherer {
	pub fn new(interval: Duration) -> Self {
		let callback: Callback = Default::default();
		Self {
			loop_handle: Some(tokio::spawn(Self::gather_loop(interval, callback.clone()))),
			callback,
		}
	}

	pub fn set_callback(&self, callback: impl Fn(EnergyMetrics) + Send + Sync + 'static) {
		*self.callback.write().unwrap() = Some(Box::new(callback));
	}

	async fn gather_loop(interval: Duration, callback: Callback) {
		let sensor = scaphandre::get_default_sensor();
		let mut topo = sensor
			.get_topology()
			.expect("sensor topology should be available");

		loop {
			topo.refresh();

			for pid in topo.proc_tracker.get_alive_pids() {
				let cmdline = topo.proc_tracker.get_process_cmdline(pid);

				if let Some(cmdline) = cmdline {
					if cmdline.contains("qemu") {
						if let Some(metric) = topo.get_process_power_consumption_microwatts(pid) {
							let power = f64::from_str(&metric.value).unwrap() / 1_000_000.;
							if let Some(callback) = &*callback.read().expect("Lock is poisoned") {
								callback(EnergyMetrics { power_usage: power });
							}
						} else {
							warn!("No power consumption metric found for PID {}", pid);
						}
					}
				}
			}
			sleep(interval).await;
		}
	}
}

impl Drop for EnergyMetricsGatherer {
	fn drop(&mut self) {
		if let Some(handle) = self.loop_handle.take() {
			handle.abort();
		}
	}
}
