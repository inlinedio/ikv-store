use crate::controller::external_handle;
use crate::controller::main::Controller;
use crate::proto::generated_proto::common::IKVStoreConfig;

pub fn open_index(ikv_config: &IKVStoreConfig) -> anyhow::Result<i64> {
    // configure logging
    crate::utils::logging::configure_logging(&ikv_config)?;

    // create and startup controller
    let controller = Controller::open(&ikv_config)?;

    Ok(external_handle::to_external_handle(controller))
}

pub fn close_index(handle: i64) -> anyhow::Result<()> {
    let boxed_controller = external_handle::to_box(handle);
    boxed_controller.close()
}
