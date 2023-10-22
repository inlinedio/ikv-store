use jni::objects::JByteArray;
use jni::sys::jbyteArray;
use jni::JNIEnv;

pub fn jbyte_array_to_vec<'local>(env: &JNIEnv<'local>, jbytes: JByteArray) -> Vec<u8> {
    let size = env.get_array_length(&jbytes).unwrap();
    let mut result = vec![0 as i8; size as usize];
    env.get_byte_array_region(jbytes, 0, &mut result);
    vec_i8_into_u8(result)
}

pub fn vec_to_jbyte_array<'local>(env: &JNIEnv<'local>, bytes: Vec<u8>) -> jbyteArray {
    let result = env.new_byte_array(bytes.len() as i32).unwrap();
    let bytes = vec_u8_into_i8(bytes);
    env.set_byte_array_region(&result, 0, &bytes);
    result.into_raw()
}

/// https://stackoverflow.com/questions/59707349/cast-vector-of-i8-to-vector-of-u8-in-rust
fn vec_i8_into_u8(v: Vec<i8>) -> Vec<u8> {
    // ideally we'd use Vec::into_raw_parts, but it's unstable,
    // so we have to do it manually:

    // first, make sure v's destructor doesn't free the data
    // it thinks it owns when it goes out of scope
    let mut v = std::mem::ManuallyDrop::new(v);

    // then, pick apart the existing Vec
    let p = v.as_mut_ptr();
    let len = v.len();
    let cap = v.capacity();

    // finally, adopt the data into a new Vec
    unsafe { Vec::from_raw_parts(p as *mut u8, len, cap) }
}

fn vec_u8_into_i8(v: Vec<u8>) -> Vec<i8> {
    // ideally we'd use Vec::into_raw_parts, but it's unstable,
    // so we have to do it manually:

    // first, make sure v's destructor doesn't free the data
    // it thinks it owns when it goes out of scope
    let mut v = std::mem::ManuallyDrop::new(v);

    // then, pick apart the existing Vec
    let p = v.as_mut_ptr();
    let len = v.len();
    let cap = v.capacity();

    // finally, adopt the data into a new Vec
    unsafe { Vec::from_raw_parts(p as *mut i8, len, cap) }
}
