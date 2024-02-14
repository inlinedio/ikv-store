use std::ffi::CStr;

#[repr(C)]
pub struct BytesBuffer {
    data: *mut u8,
    len: usize,
}

#[no_mangle]
pub extern "C" fn hello_world(name: *const libc::c_char) {
    let name_cstr = unsafe { CStr::from_ptr(name) };
    let name = name_cstr.to_str().unwrap();
    println!("Hello world from Rust: {}!", name);
    return;
}

/*
#[no_mangle]
pub extern "C" fn open(config: BytesBuffer) -> i64 {
    println!("open");
    0
}*/

#[no_mangle]
pub extern "C" fn close(handle: i64) {
    println!("close");
}

#[no_mangle]
pub extern "C" fn read_field(handle: i64, primary_key: BytesBuffer, field_name: *const libc::c_char) -> BytesBuffer {
    BytesBuffer { data: vec![0u8].as_mut_ptr(), len: 1 }
}