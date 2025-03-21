use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::f64::consts::PI;

#[derive(Clone)]
pub struct PowerGenerator {
	consumption: f64,
	variance: f64,
	on_time: (usize, usize),
	off_time: (usize, usize),

	rand: SmallRng,

	next_transition: usize,
	state: bool,
	iteration: usize,
}

impl PowerGenerator {
	/// Create a new PowerGenerator.
	///
	/// # Arguments
	///
	/// * `consumption` - The base power consumption of the appliance.
	/// * `variance` - The variance of the power consumption.
	/// * `on_time` - The range of time the appliance is on.
	/// * `off_time` - The range of time the appliance is off.
	/// * `seed` - The seed for the random number generator.
	pub fn new(
		consumption: f64,
		variance: f64,
		on_time: (usize, usize),
		off_time: (usize, usize),
		seed: u64,
	) -> Self {
		Self {
			consumption,
			variance,
			on_time,
			off_time,
			rand: SmallRng::seed_from_u64(seed),
			next_transition: 0,
			state: false,
			iteration: 0,
		}
	}

	pub fn from_device_name(name: &str, seed: u64) -> Option<Self> {
		match name {
			"Refrigerator" => Some(Self::new(150.0, 10.0, (30, 60), (10, 20), seed)),
			"AirConditioner" => Some(Self::new(2000.0, 100.0, (60, 120), (30, 60), seed)),
			"Heater" => Some(Self::new(1500.0, 50.0, (60, 120), (30, 60), seed)),
			"WashingMachine" => Some(Self::new(500.0, 50.0, (20, 40), (60, 120), seed)),
			"Dishwasher" => Some(Self::new(1800.0, 100.0, (30, 60), (120, 240), seed)),
			"Microwave" => Some(Self::new(1200.0, 50.0, (5, 10), (30, 60), seed)),
			"Oven" => Some(Self::new(2400.0, 100.0, (30, 60), (60, 120), seed)),
			"Toaster" => Some(Self::new(800.0, 20.0, (1, 2), (10, 20), seed)),
			"CoffeeMaker" => Some(Self::new(1000.0, 30.0, (5, 10), (20, 40), seed)),
			"Television" => Some(Self::new(100.0, 10.0, (60, 120), (30, 60), seed)),
			"Lights" => Some(Self::new(60.0, 5.0, (60, 120), (30, 60), seed)),
			_ => None,
		}
	}

	pub fn get_random_device(rand: &mut SmallRng) -> Self {
		let devices = [
			"Refrigerator",
			"AirConditioner",
			"Heater",
			"WashingMachine",
			"Dishwasher",
			"Microwave",
			"Oven",
			"Toaster",
			"CoffeeMaker",
			"Television",
			"Lights",
		];
		let device = devices[rand.random_range(0..devices.len())];
		Self::from_device_name(device, rand.random()).unwrap()
	}
}

impl Iterator for PowerGenerator {
	type Item = f64;

	/// Generate the next power consumption value.
	fn next(&mut self) -> Option<Self::Item> {
		if self.iteration == self.next_transition {
			self.state = !self.state;
			self.next_transition = self.iteration
				+ if self.state {
					self.rand.random_range(self.on_time.0..self.on_time.1)
				} else {
					self.rand.random_range(self.off_time.0..self.off_time.1)
				};
		}
		self.iteration += 1;
		Some(self.consumption + self.rand.random_range(-self.variance..self.variance))
	}
}

pub struct TemperatureGenerator {
	pub period: usize,
	pub day_night_difference: f64,
	pub shift: f64,
	pub offset: f64,
	pub distortion: f64,
	pub variance: f64,

	time: usize,
	rand: SmallRng,
}

impl TemperatureGenerator {
	/// Create a new TemperatureGenerator.
	///
	/// # Arguments
	///
	/// * `period` - The period of the temperature oscillation.
	/// * `day_night_difference` - The difference between the day and night temperatures.
	/// * `shift` - The shift of the temperature oscillation.
	/// * `offset` - The temperature at night.
	/// * `distortion` - The distortion of the sinusoidal temperature oscillation.
	/// * `variance` - The variance of the temperature.
	/// * `seed` - The seed for the random number generator.
	pub(crate) fn new(
		period: usize,
		day_night_difference: f64,
		shift: f64,
		offset: f64,
		distortion: f64,
		variance: f64,
		seed: u64,
	) -> Self {
		Self {
			period,
			day_night_difference,
			shift,
			offset,
			distortion,
			variance,
			time: 0,
			rand: SmallRng::seed_from_u64(seed),
		}
	}
}

impl Iterator for TemperatureGenerator {
	type Item = f64;

	/// Generate the next temperature value.
	///
	/// This implements formula 4.1 from the thesis.
	fn next(&mut self) -> Option<Self::Item> {
		Some(
			self.day_night_difference / 2.0
				* (f64::tanh(
					f64::sin((2.0 * PI * self.time as f64) / self.period as f64 + self.shift)
						* self.distortion * (self.period as f64 / (2.0 * PI)),
				) + 1.0) + self.offset
				+ self.rand.random_range(-self.variance..self.variance),
		)
	}
}

type EventFn<T, U> = Box<dyn Fn(&mut SmallRng, &mut U) -> T + Send>;

pub struct EventGenerator<T, U> {
	event_probability: f64,
	generate_event: EventFn<T, U>,
	state: U,

	rand: SmallRng,
}

impl<T, U> EventGenerator<T, U> {
	/// Create a new EventGenerator.
	///
	/// # Arguments
	///
	/// * `event_fn` - The function that generates the event.
	/// * `event_probability` - The probability of an event occurring.
	/// * `seed` - The seed for the random number generator.
	/// * `state` - The state of the event generator.
	pub fn new(
		event_fn: impl (Fn(&mut SmallRng, &mut U) -> T) + Send + 'static,
		event_probability: f64,
		seed: u64,
		state: U,
	) -> Self {
		Self {
			event_probability,
			generate_event: Box::new(event_fn),
			rand: SmallRng::seed_from_u64(seed),
			state,
		}
	}
}

impl<T, U> Iterator for EventGenerator<T, U> {
	type Item = Option<T>;

	fn next(&mut self) -> Option<Self::Item> {
		if self.rand.random_bool(self.event_probability) {
			Some(Some((self.generate_event)(&mut self.rand, &mut self.state)))
		} else {
			Some(None)
		}
	}
}

pub struct DoorWindowGenerator {
	gen: EventGenerator<DoorWindowEvent, DoorWindowEvent>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum DoorWindowEvent {
	Open,
	Close,
}

impl DoorWindowGenerator {
	pub fn new(event_probability: f64, seed: u64) -> Self {
		Self {
			gen: EventGenerator::new(
				|_, last_event| {
					let new = match last_event {
						DoorWindowEvent::Open => DoorWindowEvent::Close,
						DoorWindowEvent::Close => DoorWindowEvent::Open,
					};
					*last_event = new;
					new
				},
				event_probability,
				seed,
				DoorWindowEvent::Close,
			),
		}
	}
}

impl Iterator for DoorWindowGenerator {
	type Item = Option<DoorWindowEvent>;

	fn next(&mut self) -> Option<Self::Item> {
		self.gen.next()
	}
}

pub struct LightGenerator {
	gen: EventGenerator<f64, ()>,
}

impl LightGenerator {
	pub fn new(event_probability: f64, seed: u64) -> Self {
		Self {
			gen: EventGenerator::new(
				|rand, _| rand.random_range(0.0..1.0),
				event_probability,
				seed,
				(),
			),
		}
	}
}

impl Iterator for LightGenerator {
	type Item = Option<f64>;

	fn next(&mut self) -> Option<Self::Item> {
		self.gen.next()
	}
}
