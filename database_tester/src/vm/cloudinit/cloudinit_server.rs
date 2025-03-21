use crate::vm::cloudinit::cloudinit_generator::CloudInitGenerator;
use crate::vm::vm_manager::VMManagerInner;
use actix_web::middleware::Logger;
use actix_web::web::{Data, Path};
use actix_web::{get, App, HttpRequest, HttpResponse, HttpServer};
use anyhow::anyhow;
use log::{debug, error, info, warn};
use std::io;
use std::net::IpAddr;
use std::sync::{Arc, Weak};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

/// This struct is responsible for serving cloud-init files via HTTP.
pub struct CloudInitServer {
	addr: IpAddr,
	port: u16,
	handle: Mutex<Option<JoinHandle<io::Result<()>>>>,
	generator: Arc<CloudInitGenerator>,
	manager: Weak<VMManagerInner>,
}

impl CloudInitServer {
	pub fn new(manager: Weak<VMManagerInner>, addr: IpAddr, port: u16) -> Self {
		Self {
			addr,
			port,
			handle: Default::default(),
			generator: Arc::new(CloudInitGenerator::new(manager.clone())),
			manager,
		}
	}
}

#[get("/{name}/{file}")]
async fn handler(
	path: Path<(String, String)>,
	generator: Data<CloudInitGenerator>,
	manager: Data<Weak<VMManagerInner>>,
	req: HttpRequest,
) -> HttpResponse {
	let (name, file) = path.into_inner();

	if let Some(ip) = req.peer_addr().map(|addr| addr.ip()) {
		if let Some(manager) = manager.upgrade() {
			if let Some(vm) = manager.get_vm_by_name(&name) {
				vm.set_ip_addr(ip);
			}
		}
	}

	let result = match file.as_str() {
		"meta-data" => generator.generate_meta_data(&name),
		"user-data" => generator.generate_user_data(&name),
		"vendor-data" => generator.generate_vendor_data(&name),
		_ => Err(anyhow!("Unknown file requested")),
	};

	if let Ok(data) = &result {
		debug!("CloudInit request for {name}/{file}:\n{data}");
	} else {
		error!("Failed to generate cloudinit response: {result:?}");
	}

	match result {
		Ok(data) => HttpResponse::Ok().content_type("text/plain").body(data),
		Err(error) => {
			error!("Failed to generate cloudinit response: {error:?}");
			HttpResponse::InternalServerError().finish()
		}
	}
}

impl CloudInitServer {
	pub(crate) async fn run(&self) -> io::Result<()> {
		if self.handle.lock().await.is_some() {
			warn!("CloudInitServer was already running!");
			return Ok(());
		}

		let generator_data = Data::from(self.generator.clone());
		let manager_data = Data::new(self.manager.clone());

		let server = HttpServer::new(move || {
			App::new()
				.app_data(generator_data.clone())
				.app_data(manager_data.clone())
				.service(handler)
				.wrap(Logger::default())
		})
		.bind((self.addr, self.port))?
		.disable_signals()
		.run();

		info!("Starting CloudInitServer on {}:{}", self.addr, self.port);
		*self.handle.lock().await = Some(tokio::spawn(server));

		Ok(())
	}
}
