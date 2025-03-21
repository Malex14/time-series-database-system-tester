use crate::vm::cloudinit::cloudinit_server::CloudInitServer;
use crate::vm::vm_config::VMConfig;
use crate::vm::{Net, VM};
use anyhow::anyhow;
use log::{debug, error, info};
use std::cell::SyncUnsafeCell;
use std::collections::HashMap;
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr};
use std::ops::Deref;
use std::path::Path;
use std::process::Command;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::thread::JoinHandle;
use std::time::SystemTime;
use std::{io, thread};
use uuid::Uuid;
use virt::connect::Connect;
use virt::domain::Domain;
use virt::network::Network;
use virt::sys::{
	virEventRegisterDefaultImpl, virEventRunDefaultImpl, VIR_DOMAIN_START_AUTODESTROY,
	VIR_DOMAIN_START_RESET_NVRAM,
};

/// This struct represents a virtual machine manager that is responsible for managing the VMs.
///
/// It handles the connection to the hypervisor via libvirt and manages the cloud-init server.
pub struct VMManager {
	inner: Arc<VMManagerInner>,
	_cloud_init_server: CloudInitServer,
}

impl VMManager {
	/// Creates a new VMManager.
	///
	/// # Arguments
	/// * `config_path` - Path to the configuration file.
	/// * `virt_uri` - The URI of the hypervisor. If `None`, the default URI will be used.
	pub async fn new(
		config_path: impl AsRef<Path>,
		virt_uri: Option<&str>,
	) -> anyhow::Result<Self> {
		let inner = Arc::new(VMManagerInner::new(config_path, virt_uri)?);
		let cloud_init_server = CloudInitServer::new(
			Arc::downgrade(&inner),
			Ipv4Addr::from_str(&inner.config.network.host_ip)?.into(),
			8000,
		);

		info!("Starting cloud-init server");
		cloud_init_server.run().await?;

		info!("Finished initializing VMManager");
		Ok(Self {
			inner,
			_cloud_init_server: cloud_init_server,
		})
	}
}

impl Deref for VMManager {
	type Target = VMManagerInner;

	fn deref(&self) -> &Self::Target {
		&self.inner
	}
}

pub struct VMManagerInner {
	event_thread_handle: Option<JoinHandle<()>>,
	stop_event_thread: Arc<SyncUnsafeCell<bool>>,
	connection: Connect,
	vms: RwLock<HashMap<String, VM>>,
	config: VMConfig,
	_net: Net,
}

impl VMManagerInner {
	const DEFAULT_URI: &'static str = "qemu:///system";

	fn new(config_path: impl AsRef<Path>, virt_uri: Option<&str>) -> anyhow::Result<Self> {
		let config = VMConfig::read_config(config_path)?;

		let virt_uri = virt_uri.or(Some(Self::DEFAULT_URI));

		let stop_event_thread = Arc::new(SyncUnsafeCell::new(false));

		debug!("Starting event thread");
		let handle = {
			let stop_event_thread = stop_event_thread.clone();
			thread::spawn(move || {
				unsafe { virEventRegisterDefaultImpl() };

				info!("Event loop started");

				let ptr = stop_event_thread.get();
				// This loop will be stopped, when self is dropped
				#[allow(clippy::while_immutable_condition)]
				while !unsafe { *ptr } {
					unsafe { virEventRunDefaultImpl() };
				}

				info!("Event thread stopped");
			})
		};

		info!(
			"Connecting to hypervisor at: {}",
			virt_uri.expect("some uri should be present here")
		);
		let connection = Connect::open(virt_uri)?;
		info!("Connection to hypervisor established");

		info!("Creating Network");
		if let Ok(old_net) = Network::lookup_by_name(&connection, &config.network.name) {
			info!("Network already exists, destroying it");
			old_net.destroy()?;
		}
		let net_uuid = Uuid::new_v4();
		let net_xml = format!(
			r#"
<network>
  <name>{name}</name>
  <uuid>{uuid}</uuid>
  <forward mode="nat"/>
  <domain name="{name}"/>
  <ip address="{host_ip}" netmask="255.255.255.0">
    <dhcp>
      <range start="{host_net}.128" end="{host_net}.254"/>
    </dhcp>
  </ip>
</network>
            "#,
			name = config.network.name,
			uuid = Uuid::new_v4(),
			host_ip = config.network.host_ip,
			host_net = config.network.host_ip[0..config
				.network
				.host_ip
				.rfind('.')
				.ok_or(anyhow!("Malformed host_ip"))?]
				.to_string()
		);
		debug!("{net_xml}");
		let virt_net = Network::create_xml(&connection, &net_xml)?;
		let net = Net::new(virt_net, net_uuid);
		info!("Network created");

		info!("Creating shared directory");
		let share_dir = config.temp_dir.join("share");
		if !share_dir.exists() {
			std::fs::create_dir_all(&share_dir)?;
		}

		info!("Coping instrumentation_gatherer to shared directory");
		let instrumentation_gatherer_path = share_dir.join("instrumentation_gatherer");
		std::fs::copy(
			"./bin/instrumentation_gatherer",
			&instrumentation_gatherer_path,
		)?;

		Ok(Self {
			event_thread_handle: Some(handle),
			stop_event_thread,
			connection,
			vms: Default::default(),
			config,
			_net: net,
		})
	}

	pub fn get_connection(&self) -> &Connect {
		&self.connection
	}

	pub fn get_vm_by_name(&self, name: &str) -> Option<VM> {
		self.vms
			.read()
			.expect("RwLock is poisoned")
			.get(name)
			.cloned()
	}

	/// Create a new VM
	///
	/// The VM will be automatically started
	///
	/// # Arguments
	///
	/// * `name` - Name of the VM
	/// * `num_cpus` - Number of CPUs
	/// * `memory` - Memory in MB
	/// * `auto_destroy` - If the VM should be destroyed when it is stopped
	/// * `database_config_name` - Name of the database config to use
	/// * `base_disk_path` - Path to the base disk image
	pub fn create_vm(
		&self,
		name: &str,
		num_cpus: u32,
		memory: u32,
		auto_destroy: bool,
		database_config_name: &str,
		base_disk_path: Option<&Path>,
	) -> anyhow::Result<VM> {
		if self
			.vms
			.read()
			.expect("RwLock is poisoned")
			.contains_key(name)
		{
			return Err(anyhow::anyhow!("VM with name {} already exists", name));
		}

		let uuid = Uuid::new_v4();
		let cloudinit_server: IpAddr = Ipv4Addr::from_str(&self.config.network.host_ip)?.into();
		let disk_path = self.config.temp_dir.join(format!(
			"vm-{name}-image-{}.qcow2",
			SystemTime::now()
				.duration_since(SystemTime::UNIX_EPOCH)?
				.as_secs()
		));
		let disk_path_str = disk_path.to_string_lossy();

		info!("Creating disk image at: {}", disk_path_str);
		let qemu_img_output = Command::new("qemu-img")
			.args([
				"create",
				"-f",
				"qcow2",
				"-F",
				"qcow2",
				"-b",
				&*base_disk_path
					.unwrap_or(&self.config.base_disk_image.canonicalize()?)
					.to_string_lossy(),
				"-o",
				&format!("size={}", self.config.disk_size),
				&*disk_path_str,
			])
			.output()?;
		io::stdout().write_all(&qemu_img_output.stdout)?;
		io::stderr().write_all(&qemu_img_output.stderr)?;

		let xml = format!(
			include_str!("../../vm_template.xml"),
			name = name,
			uuid = uuid,
			num_cpus = num_cpus,
			memory = memory,
			cloudinit_server = cloudinit_server,
			disk_path = disk_path_str,
			temp_dir = self.config.temp_dir.to_string_lossy(),
			net_name = self.config.network.name,
		);

		debug!("{xml}");

		info!("Creating VM with name: {}", name);
		let mut flags = VIR_DOMAIN_START_RESET_NVRAM;
		if auto_destroy {
			flags |= VIR_DOMAIN_START_AUTODESTROY
		}
		let domain = Domain::create_xml(&self.connection, &xml, flags)?;
		info!("VM with name {} created", name);

		let vm = VM::new(
			domain,
			uuid,
			self.config.temp_dir.clone(),
			self.config
				.database
				.iter()
				.find(|config| config.name == database_config_name)
				.ok_or(anyhow!(
					"Database config \"{database_config_name}\" not found"
				))?
				.clone(),
			disk_path,
		);
		let mut vms = self.vms.write().expect("RwLock is poisoned");
		vms.insert(name.to_string(), vm.clone());

		Ok(vm)
	}
}

impl Drop for VMManagerInner {
	fn drop(&mut self) {
		info!("Stopping VMManager");

		info!("Closing connection to hypervisor");
		if let Err(error) = self.connection.close() {
			error!("Error while closing connection to hypervisor: {error:?}");
		}

		info!("Stopping event thread");
		// SAFETY: This is safe because race conditions do not matter here since we write only one byte.
		unsafe {
			*self.stop_event_thread.get() = true;
		}

		if let Err(error) = self
			.event_thread_handle
			.take()
			.expect("event_thread_handle option was empty when dropping")
			.join()
		{
			error!("Event thread did panic at some point: {error:?}");
		}

		info!("VMManager stopped");
	}
}
