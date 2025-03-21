mod cloudinit;
mod vm_config;
pub mod vm_manager;

use crate::vm::vm_config::DatabaseConfig;
use log::error;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use uuid::Uuid;
use virt::domain::Domain;
use virt::network::Network;
use virt::sys::{
	VIR_DOMAIN_BLOCKED, VIR_DOMAIN_CRASHED, VIR_DOMAIN_NOSTATE, VIR_DOMAIN_PAUSED,
	VIR_DOMAIN_PMSUSPENDED, VIR_DOMAIN_RUNNING, VIR_DOMAIN_SHUTDOWN, VIR_DOMAIN_SHUTOFF,
};

/// This struct represents a virtual machine that is managed by the VM manager.
#[derive(Debug, Clone)]
pub struct VM {
	domain: Arc<Domain>,
	config: Arc<DatabaseConfig>,
	id: Uuid,
	temp_dir: Arc<PathBuf>,
	ip_addr: Arc<RwLock<Option<IpAddr>>>,
	disk_path: PathBuf,
}

impl VM {
	fn new(
		domain: Domain,
		id: Uuid,
		temp_dir: PathBuf,
		config: DatabaseConfig,
		disk_path: PathBuf,
	) -> Self {
		Self {
			domain: Arc::new(domain),
			config: Arc::new(config),
			id,
			temp_dir: Arc::new(temp_dir),
			ip_addr: Default::default(),
			disk_path,
		}
	}

	pub fn get_name(&self) -> String {
		self.domain.get_name().expect("get_name() for VM failed")
	}

	pub fn get_init_commands(&self) -> Vec<&str> {
		self.config.init_commands.split("\n").collect()
	}

	pub fn stop(&self) -> anyhow::Result<()> {
		self.domain.shutdown()?;

		Ok(())
	}

	pub fn poweroff(&self) -> anyhow::Result<()> {
		self.domain.destroy()?;

		Ok(())
	}

	pub fn get_state(&self) -> anyhow::Result<VMState> {
		Ok(match self.domain.get_state()?.0 {
			VIR_DOMAIN_NOSTATE => VMState::NOSTATE,
			VIR_DOMAIN_RUNNING => VMState::RUNNING,
			VIR_DOMAIN_BLOCKED => VMState::BLOCKED,
			VIR_DOMAIN_PAUSED => VMState::PAUSED,
			VIR_DOMAIN_SHUTDOWN => VMState::SHUTDOWN,
			VIR_DOMAIN_SHUTOFF => VMState::SHUTOFF,
			VIR_DOMAIN_CRASHED => VMState::CRASHED,
			VIR_DOMAIN_PMSUSPENDED => VMState::PMSUSPENDED,
			_ => unreachable!("vm has unknown state"),
		})
	}

	pub fn get_id(&self) -> Uuid {
		self.id
	}

	pub fn get_channel(&self) -> String {
		self.temp_dir
			.join(format!("vm-{}-metrics", self.get_name()))
			.to_string_lossy()
			.to_string()
	}

	pub fn get_ip_addr(&self) -> Option<IpAddr> {
		*self.ip_addr.read().expect("RwLock is poisoned")
	}

	pub fn set_ip_addr(&self, ip_addr: IpAddr) {
		*self.ip_addr.write().expect("RwLock is poisoned") = Some(ip_addr);
	}

	pub fn get_disk_path(&self) -> &Path {
		&self.disk_path
	}

	pub fn get_password(&self) -> Option<&str> {
		self.config.password.as_deref()
	}
}

// adapted from: https://libvirt.org/html/libvirt-libvirt-domain.html#virDomainState
#[derive(Debug)]
#[allow(clippy::upper_case_acronyms)]
pub enum VMState {
	/// no state
	NOSTATE,
	/// the vm is running
	RUNNING,
	/// the vm is blocked on resource
	BLOCKED,
	/// the vm is paused by user
	PAUSED,
	/// the vm is being shut down
	SHUTDOWN,
	/// the vm is shut off
	SHUTOFF,
	/// the vm is crashed
	CRASHED,
	/// the vm is suspended by guest power management
	PMSUSPENDED,
}

/// This struct represents a network that is managed by the VM manager.
pub struct Net {
	network: Network,
	_uuid: Uuid,
}

impl Net {
	pub fn new(network: Network, uuid: Uuid) -> Self {
		Self {
			network,
			_uuid: uuid,
		}
	}
}

impl Drop for Net {
	fn drop(&mut self) {
		if let Err(error) = self.network.destroy() {
			error!("Error while destroying network: {error:?}");
		}
	}
}
