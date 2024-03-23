use std::ffi::CStr;

use protobuf::Message;

use crate::{
    controller::{external_handle, main::Controller},
    proto::generated_proto::common::IKVStoreConfig,
};

#[no_mangle]
pub extern "C" fn health_check(input: *const libc::c_char) -> i64 {
    let name_cstr = unsafe { CStr::from_ptr(input) };
    match name_cstr.to_str() {
        Ok(_) => 0,
        Err(_) => -1,
    }
}

#[no_mangle]
pub extern "C" fn open_index(config: *const libc::c_char, config_len: i32) -> i64 {
    let cfg_bytes = unsafe { std::slice::from_raw_parts(config as *const u8, config_len as usize) };

    // TODO! remove all the unwraps and propagate errors.

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

// References:
// https://users.rust-lang.org/t/how-to-return-byte-array-from-rust-function-to-ffi-c/18136/4
// https://kmdouglass.github.io/posts/complex-data-types-and-the-rust-ffi/
// https://github.com/mozilla/cbindgen

#[repr(C)]
pub struct BytesBuffer {
    length: i32,
    start: *mut u8,
}

#[no_mangle]
pub extern "C" fn get_field_value(
    handle: i64,
    pkey: *const libc::c_char,
    pkey_len: i32,
    field_name: *const libc::c_char,
) -> BytesBuffer {
    let controller = external_handle::from_external_handle(handle);
    let primary_key = unsafe { std::slice::from_raw_parts(pkey as *const u8, pkey_len as usize) };
    let field_name = (unsafe { CStr::from_ptr(field_name) }).to_str().unwrap();

    let maybe_field_value = controller
        .index_ref()
        .get_field_value(&primary_key, &field_name);
    if maybe_field_value.is_none() {
        return BytesBuffer {
            length: 0,
            start: std::ptr::null_mut(),
        };
    }

    // empty value is handled as missing field value
    let field_value = maybe_field_value.unwrap();
    if field_value.is_empty() {
        return BytesBuffer {
            length: 0,
            start: std::ptr::null_mut(),
        };
    }

    let mut field_value = field_value.into_boxed_slice();
    let field_value_ptr = field_value.as_mut_ptr();
    let length = field_value.len() as i32;

    std::mem::forget(field_value);
    BytesBuffer {
        length,
        start: field_value_ptr,
    }
}

#[no_mangle]
pub extern "C" fn free_bytes_buffer(buf: BytesBuffer) {
    if buf.start.is_null() || buf.length == 0 {
        return;
    }

    let buffer = unsafe { std::slice::from_raw_parts_mut(buf.start, buf.length as usize) };
    let buffer = buffer.as_mut_ptr();
    unsafe {
        drop(Box::from_raw(buffer));
    }
}
