use std::ffi::CStr;

use protobuf::Message;

use crate::{
    controller::{external_handle, main::Controller},
    proto::generated_proto::common::IKVStoreConfig,
};

#[no_mangle]
pub extern "C" fn hello_world_a(input: *const libc::c_char) {
    let name_cstr = unsafe { CStr::from_ptr(input) };
    let name = name_cstr.to_str().unwrap();
    println!("Hello world A from Rust: {}!", name);
}

#[no_mangle]
pub extern "C" fn open_index(config: *const libc::c_char, config_len: i32) -> i64 {
    let cfg_bytes = unsafe { std::slice::from_raw_parts(config as *const u8, config_len as usize) };

    // deserialize configs
    let ikv_config = IKVStoreConfig::parse_from_bytes(cfg_bytes).expect("cannot deser");

    // configure logging
    crate::utils::logging::configure_logging(&ikv_config).unwrap();

    // create and startup controller
    let controller = Controller::open(&ikv_config).unwrap();

    external_handle::to_external_handle(controller)
}

#[no_mangle]
pub extern "C" fn close_index(handle: i64) {
    let boxed_controller = external_handle::to_box(handle);
    if let Err(e) = boxed_controller.close() {
        eprintln!("Cannot close reader, failed with error: {}", e.to_string());
    }
}
