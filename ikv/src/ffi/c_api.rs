use std::ffi::CStr;

use log::error;
use protobuf::Message;

use crate::proto::generated_proto::common::IKVStoreConfig;

use crate::controller::external_handle;

use crate::ffi::api;

#[no_mangle]
pub extern "C" fn health_check(input: *const libc::c_char) -> i64 {
    let name_cstr = unsafe { CStr::from_ptr(input) };
    match name_cstr.to_str() {
        Ok(_) => 0,
        Err(_) => -1,
    }
}

#[repr(C)]
pub struct IndexHandle {
    // handle for subsequent api calls
    // valid only iff status=0
    handle: i64,

    // initialization status
    // success -> status=0
    // failure -> status=error_code
    status: i64,
}

#[deprecated]
#[no_mangle]
pub extern "C" fn open_index(config: *const libc::c_char, config_len: i32) -> i64 {
    let cfg_bytes = unsafe { std::slice::from_raw_parts(config as *const u8, config_len as usize) };

    // deserialize configs
    let ikv_config =
        IKVStoreConfig::parse_from_bytes(cfg_bytes).expect("cannot deserialize client options");

    api::open_index(&ikv_config).expect("IKV startup error")
}

#[no_mangle]
pub extern "C" fn open_index_v2(config: *const libc::c_char, config_len: i32) -> IndexHandle {
    let cfg_bytes = unsafe { std::slice::from_raw_parts(config as *const u8, config_len as usize) };

    // parse configs
    let ikv_config;
    match IKVStoreConfig::parse_from_bytes(cfg_bytes) {
        Ok(c) => ikv_config = c,
        Err(e) => {
            eprintln!(
                "Cannot parse client_options (proto3 deser error), details: {}",
                e.to_string()
            );
            return IndexHandle {
                handle: -1,
                status: 1,
            };
        }
    }

    let handle;
    match api::open_index(&ikv_config) {
        Ok(h) => handle = h,
        Err(e) => {
            error!("Cannot startup IKV reader, details: {}", e.to_string());
            return IndexHandle {
                handle: -1,
                status: 2,
            };
        }
    }

    return IndexHandle { handle, status: 0 };
}

#[no_mangle]
pub extern "C" fn close_index(handle: i64) {
    if let Err(e) = api::close_index(handle) {
        error!("Cannot close reader, failed with error: {}", e.to_string());
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
