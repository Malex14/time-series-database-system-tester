use anyhow::anyhow;
use common::metrics::SystemMetrics;
use log::{error, info, warn};
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::UnixStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

type Callback = Arc<RwLock<Option<Box<dyn Fn(SystemMetrics) + Send + Sync>>>>;

pub struct MetricsReceiver {
	callback: Callback,
	channel: String,
	listener: Option<(JoinHandle<()>, oneshot::Sender<()>)>,
}

impl MetricsReceiver {
	pub fn new(channel: &str) -> Self {
		Self {
			callback: Default::default(),
			channel: channel.to_string(),
			listener: None,
		}
	}

	pub fn set_callback(&self, callback: impl Fn(SystemMetrics) + Send + Sync + 'static) {
		*self.callback.write().unwrap() = Some(Box::new(callback));
	}

	pub fn listen(&mut self) -> anyhow::Result<()> {
		if self.listener.is_some() {
			return Err(anyhow!("Listener is already running"));
		}

		let channel = self.channel.clone();
		let callback = self.callback.clone();

		let (tx, mut rx) = oneshot::channel();

		let handle = tokio::spawn(async move {
			match UnixStream::connect(&channel).await {
				Ok(socket) => {
					let reader = BufReader::new(socket);
					let mut lines = reader.lines();
					loop {
						tokio::select! {
							line = lines.next_line() => {
								match line {
									Ok(Some(line)) => match SystemMetrics::from_str(&line) {
										Ok(metrics) => {
											if let Some(ref callback) = *callback.read().expect("Lock is poisoned") {
												callback(metrics);
											} else {
												warn!("No metrics callback set!")
											}
										}
										Err(error) => {
											error! {"Malformed metrics line: {error:?}"}
										}
									},
									Ok(None) => {
										warn!("Socket ended unexpectedly");
										break;
									}
									Err(error) => {
										error!("Error while reading line from socket: {error:?}")
									}
								}
							}
							_ = &mut rx => {
								info!("Stopping MetricsReceiver listener");
								break;
							}
						}
					}
				}
				Err(error) => {
					error!("Error connecting to unix-socket: {error:?}");
				}
			}
		});

		self.listener = Some((handle, tx));

		Ok(())
	}
}

impl Drop for MetricsReceiver {
	fn drop(&mut self) {
		if let Some((_, tx)) = self.listener.take() {
			let _ = tx.send(());
		}
	}
}
