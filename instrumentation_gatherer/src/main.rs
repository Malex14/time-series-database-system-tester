mod metrics;

use crate::metrics::gatherer::MetricsGatherer;
use crate::metrics::sender::MetricsSender;
use clap::{arg, Parser};
use env_logger::Env;
use log::{info, warn};

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
	#[arg(
		short,
		long,
		required = true,
		help = "The device to which the metrics are send"
	)]
	channel_dev: String,

	#[arg(
		short,
		long,
		default_value_t = 2,
		help = "Rate to gather information in Hz"
	)]
	rate: u64,
}

fn main() {
	env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

	let args = Args::parse();
	let channel = args.channel_dev;
	let rate = args.rate;

	if rate > 5 {
		warn!("CPU usage might me inaccurate with rates > 5Hz");
	}

	info!("Starting metrics_gatherer on serial port \"{channel}\"");

	let sender = MetricsSender::new(&channel).unwrap();
	let mut gatherer = MetricsGatherer::new(sender, rate);

	gatherer.run()
}
