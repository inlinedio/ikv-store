use crate::controller::main::{ReadController, WriteController};
use crate::proto::generated_proto::common::IKVStoreConfig;

pub fn open_reader(ikv_config: &IKVStoreConfig) -> anyhow::Result<i64> {
    // configure logging
    crate::utils::logging::configure_logging(&ikv_config)?;

    // create and startup controller
    let controller = ReadController::open(&ikv_config)?;

    Ok(controller.to_external_handle())
}

pub fn close_reader(handle: i64) -> anyhow::Result<()> {
    let boxed_controller = ReadController::from_external_handle_as_boxed(handle);
    boxed_controller.close()
}

pub fn open_writer(ikv_config: &IKVStoreConfig) -> anyhow::Result<i64> {
    // configure logging
    crate::utils::logging::configure_logging(&ikv_config)?;

    // create and startup controller
    let controller = WriteController::open(&ikv_config)?;

    Ok(controller.to_external_handle())
}

pub fn close_writer(handle: i64) -> anyhow::Result<()> {
    let boxed_controller = WriteController::from_external_handle_as_boxed(handle);
    boxed_controller.close()
}
