use std::ffi::CStr;

use protobuf::Message;

use crate::proto::generated_proto::common::IKVStoreConfig;

#[no_mangle]
pub extern "C" fn hello_world(input: *const libc::c_char) {
    let name_cstr = unsafe { CStr::from_ptr(input) };
    let name = name_cstr.to_str().unwrap();
    println!("Hello world from Rust: {}!", name);
}



/* 

#[no_mangle]
pub extern "C" fn open(config: *const libc::c_uchar) -> libc::c_long {
    println!("Here1");

    // read size of config bytes
    let cfg_size = unsafe { std::slice::from_raw_parts(config as *const u8, 4) };
    let cfg_size = i32::from_le_bytes(cfg_size.try_into().unwrap());

    println!("Here2");

    // read config
    let cfg_start = (config as u8 + 4) as *const libc::c_uchar;
    let cfg_bytes = unsafe { std::slice::from_raw_parts(cfg_start as *const u8, cfg_size as usize) };

    println!("Here3");

    // deserialize to proto
    let ikv_config = IKVStoreConfig::parse_from_bytes(cfg_bytes).expect("cannot deser");

    println!("open called, config: {}", ikv_config.to_string());


    // todo! change.
    0i64
}

#[no_mangle]
pub extern "C" fn close(handle: libc::c_long) {
    println!("close called, handle: {}", handle);
}*/