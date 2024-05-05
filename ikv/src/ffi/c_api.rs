use std::ffi::CStr;

use log::error;
use protobuf::Message;

use crate::controller::main::ReadController;
use crate::proto::generated_proto::common::IKVStoreConfig;

use crate::ffi::{api, utils};

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

    api::open_reader(&ikv_config).expect("IKV startup error")
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
    match api::open_reader(&ikv_config) {
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
    if let Err(e) = api::close_reader(handle) {
        error!("Cannot close reader, failed with error: {}", e.to_string());
    }
}

// References:
// https://users.rust-lang.org/t/how-to-return-byte-array-from-rust-function-to-ffi-c/18136/4
// https://kmdouglass.github.io/posts/complex-data-types-and-the-rust-ffi/
// https://github.com/mozilla/cbindgen

const EMPTY_BB: BytesBuffer = BytesBuffer {
    length: 0,
    start: std::ptr::null_mut(),
};

#[repr(C)]
pub struct BytesBuffer {
    length: i32,
    start: *mut u8,
}

impl BytesBuffer {
    fn from_bytes(input: Vec<u8>) -> Self {
        if input.is_empty() {
            return EMPTY_BB;
        }

        let mut boxed_input = input.into_boxed_slice();
        let input_ptr = boxed_input.as_mut_ptr();
        let length = boxed_input.len() as i32;
        std::mem::forget(boxed_input);
        BytesBuffer {
            length,
            start: input_ptr,
        }
    }

    fn free(self) {
        if self.start.is_null() || self.length == 0 {
            return;
        }

        let buffer = unsafe { std::slice::from_raw_parts_mut(self.start, self.length as usize) };
        let buffer = buffer.as_mut_ptr();
        unsafe {
            drop(Box::from_raw(buffer));
        }
    }
}

#[no_mangle]
pub extern "C" fn get_field_value(
    handle: i64,
    pkey: *const libc::c_char,
    pkey_len: i32,
    field_name: *const libc::c_char,
) -> BytesBuffer {
    let controller = ReadController::from_external_handle(handle);
    let primary_key = unsafe { std::slice::from_raw_parts(pkey as *const u8, pkey_len as usize) };

    // only valid if field_name pointer points to valid utf8 encoded data
    let field_name = (unsafe { CStr::from_ptr(field_name) }).to_str().unwrap();
    match controller
        .index_ref()
        .get_field_value(&primary_key, &field_name)
    {
        Some(field_value) => BytesBuffer::from_bytes(field_value),
        None => EMPTY_BB,
    }
}

#[no_mangle]
pub extern "C" fn multiget_field_values(
    handle: i64,
    concat_primary_keys: *const libc::c_char,
    concat_primary_keys_len: i32,
    concat_field_names: *const libc::c_char,
    concat_field_names_len: i32,
) -> BytesBuffer {
    let controller = ReadController::from_external_handle(handle);

    // parse size-prefixed primary keys
    let concat_primary_keys = unsafe {
        std::slice::from_raw_parts(
            concat_primary_keys as *const u8,
            concat_primary_keys_len as usize,
        )
    };
    let primary_keys = utils::unpack_size_prefixed_bytes(&concat_primary_keys);
    if primary_keys.len() == 0 {
        return EMPTY_BB;
    }

    // parse size-prefixed field names
    let concat_field_names = unsafe {
        std::slice::from_raw_parts(
            concat_field_names as *const u8,
            concat_field_names_len as usize,
        )
    };
    let field_names = utils::unpack_size_prefixed_strs(&concat_field_names);
    if field_names.len() == 0 {
        return EMPTY_BB;
    }

    let result = controller
        .index_ref()
        .batch_get_field_values(primary_keys, field_names);

    BytesBuffer::from_bytes(result)
}

#[no_mangle]
pub extern "C" fn free_bytes_buffer(buf: BytesBuffer) {
    buf.free()
}
