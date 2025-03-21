use crate::vm::vm_manager::VMManagerInner;
use anyhow::anyhow;
use itertools::Itertools;
use std::sync::Weak;

/// This struct is responsible for generating cloud-init files.
pub(super) struct CloudInitGenerator {
	manager: Weak<VMManagerInner>,
}

impl CloudInitGenerator {
	pub(super) fn new(manager: Weak<VMManagerInner>) -> Self {
		Self { manager }
	}
}

impl CloudInitGenerator {
	/// Generates meta data for cloud-init
	pub(super) fn generate_meta_data(&self, name: &str) -> anyhow::Result<String> {
		let Some(vm) = self
			.manager
			.upgrade()
			.expect("Inner value already gone")
			.get_vm_by_name(name)
		else {
			return Err(anyhow!("Unknown VM"));
		};

		let uuid = vm.get_id().to_string();

		Ok(format!(
			r#"instance-id: vm
local-hostname: {uuid}
"#
		))
	}

	/// Generates user data for cloud-init
	pub(super) fn generate_user_data(&self, name: &str) -> anyhow::Result<String> {
		let Some(vm) = self
			.manager
			.upgrade()
			.expect("Inner value already gone")
			.get_vm_by_name(name)
		else {
			return Err(anyhow!("Unknown VM"));
		};

		let init_commands = vm
			.get_init_commands()
			.iter()
			.filter(|command| !command.is_empty())
			.map(|command| command.trim())
			.map(|command| format!("  - {command}"))
			.join("\n");
		let username = vm.config.user.as_deref().unwrap_or("");
		let password = vm.config.password.as_deref().unwrap_or("");

		Ok(format!(
			r#"#cloud-config
runcmd:
  - export DB_USERNAME={username}
  - export DB_PASSWORD={password}
  - systemctl disable --now unattended-upgrades.service ssh.service systemd-timesyncd.service
{init_commands}

bootcmd:
  - mount -t virtiofs share /mnt
  - /mnt/instrumentation_gatherer --channel-dev /dev/vport1p2 --rate 5 &

disable_root: false
ssh_pwauth: true

users:
  - name: debian
    sudo: ALL=(ALL) NOPASSWD:ALL
    plain_text_passwd: debian
    lock_passwd: false
    shell: /bin/bash
  - name: root
    lock_passwd: false
    plain_text_passwd: root"#
		))
	}

	/// Generates vendor data for cloud-init
	///
	/// The string returned by this method is always empty
	pub(super) fn generate_vendor_data(&self, _name: &str) -> anyhow::Result<String> {
		Ok("".to_string())
	}
}
