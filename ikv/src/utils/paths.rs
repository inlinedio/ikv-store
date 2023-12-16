use crate::proto::generated_proto::common::IKVStoreConfig;

use anyhow::anyhow;

/// Create path for mount_directory for internal use.
///
/// Online reads: ..path/to/user_supplied_mount_directory/<storename>/<partition>
///
/// Offline index builds: ..path/to/worker_supplied_mount_directory/<storename>/<partition>
/// where worker_supplied_mount_directory: /tmp/ikv-index-builds/epoch/
pub fn create_mount_directory(config: &IKVStoreConfig) -> anyhow::Result<String> {
    let user_supplied_mount_directory = config
        .stringConfigs
        .get("mount_directory")
        .ok_or(anyhow!("mount_directory is a required config"))?
        .to_string();

    let store_name = config
        .stringConfigs
        .get("store_name")
        .ok_or(anyhow!("store_name is a required config"))?
        .to_string();

    let partition = config
        .numericConfigs
        .get("partition")
        .copied()
        .ok_or(anyhow!("partition is a required config"))?;

    let mount_directory = format!(
        "{}/{}/{}",
        user_supplied_mount_directory, &store_name, partition
    );
    Ok(mount_directory)
}
