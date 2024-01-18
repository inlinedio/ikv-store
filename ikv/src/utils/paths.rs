use crate::proto::generated_proto::common::IKVStoreConfig;

use anyhow::anyhow;

/// Construct FQN path of the mount_directory for mounting the index.
///
/// Online reads: ..path/to/user_supplied_mount_directory/<storename>/<partition>
///
/// Offline index builds: ..path/to/worker_supplied_mount_directory/<storename>/<partition>
/// where worker_supplied_mount_directory: /tmp/ikv-index-builds/epoch/
pub fn get_index_mount_directory_fqn(config: &IKVStoreConfig) -> anyhow::Result<String> {
    let user_supplied_mount_directory = config
        .stringConfigs
        .get("mount_directory")
        .ok_or(anyhow!(
            "mount_directory is a required client-specified config"
        ))?
        .to_string();

    let store_name = config
        .stringConfigs
        .get("store_name")
        .ok_or(anyhow!("store_name is a required client-specified config"))?
        .to_string();

    // TODO: is partition really a client specified config?
    let partition = config
        .intConfigs
        .get("partition")
        .copied()
        .ok_or(anyhow!("partition is a required client-specified config"))?;

    let mount_directory = format!(
        "{}/{}/{}",
        &user_supplied_mount_directory, &store_name, partition
    );
    Ok(mount_directory)
}

pub fn get_working_mount_directory_fqn(config: &IKVStoreConfig) -> anyhow::Result<String> {
    let user_supplied_mount_directory = config
        .stringConfigs
        .get("mount_directory")
        .ok_or(anyhow!(
            "mount_directory is a required client-specified config"
        ))?
        .to_string();

    let store_name = config
        .stringConfigs
        .get("store_name")
        .ok_or(anyhow!("store_name is a required client-specified config"))?
        .to_string();

    // TODO: is partition really a client specified config?
    let partition = config
        .intConfigs
        .get("partition")
        .copied()
        .ok_or(anyhow!("partition is a required client-specified config"))?;

    let mount_directory = format!(
        "{}/working_dir/{}/{}",
        &user_supplied_mount_directory, &store_name, partition
    );
    Ok(mount_directory)
}
