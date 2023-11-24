use std::collections::HashMap;

use jni::objects::{JByteArray, JList, JObject, JString};
use jni::sys::jbyteArray;
use jni::JNIEnv;

use crate::proto::generated_proto::services::FieldValue;

pub fn jbyte_array_to_vec<'local>(env: &JNIEnv<'local>, jbytes: JByteArray) -> Vec<u8> {
    let size = env.get_array_length(&jbytes).unwrap();
    let mut result = vec![0 as i8; size as usize];
    env.get_byte_array_region(jbytes, 0, &mut result).unwrap();
    vec_i8_into_u8(result)
}

pub fn vec_to_jbyte_array<'local>(env: &JNIEnv<'local>, bytes: Vec<u8>) -> jbyteArray {
    let result = env.new_byte_array(bytes.len() as i32).unwrap();
    let bytes = vec_u8_into_i8(bytes);
    env.set_byte_array_region(&result, 0, &bytes).unwrap();
    result.into_raw()
}

/// List<byte[]> to Vec<Vec<String>>
pub fn jlist_to_vec_strings<'local>(
    env: &mut JNIEnv<'local>,
    input: JObject<'local>,
) -> Vec<String> {
    let mut results = Vec::new();
    let jlist = JList::from_env(env, &input).unwrap();
    let mut iterator = jlist.iter(env).unwrap();
    while let Some(obj) = iterator.next(env).unwrap() {
        /*
           Each call to next creates a new local reference.
           To prevent excessive memory usage or overflow error,
           the local reference should be deleted using JNIEnv::delete_local_ref or JNIEnv::auto_local
           before the next loop iteration. Alternatively,
           if the list is known to have a small, predictable size,
           the loop could be wrapped in JNIEnv::with_local_frame to delete all
           of the local references at once.
        */
        let jstring: JString = obj.into();
        let string = env.get_string(&jstring).unwrap().into();
        results.push(string);
    }

    results
}

/// List<byte[]> to Vec<Vec<u8>>
pub fn jlist_to_vec_bytes<'local>(
    env: &mut JNIEnv<'local>,
    input: JObject<'local>,
) -> Vec<Vec<u8>> {
    let mut results = Vec::new();
    let jlist = JList::from_env(env, &input).unwrap();
    let mut iterator = jlist.iter(env).unwrap();
    while let Some(obj) = iterator.next(env).unwrap() {
        /*
           Each call to next creates a new local reference.
           To prevent excessive memory usage or overflow error,
           the local reference should be deleted using JNIEnv::delete_local_ref or JNIEnv::auto_local
           before the next loop iteration. Alternatively,
           if the list is known to have a small, predictable size,
           the loop could be wrapped in JNIEnv::with_local_frame to delete all
           of the local references at once.
        */
        let jbytearray: JByteArray = obj.into();
        let vec_bytes = env.convert_byte_array(jbytearray).unwrap();
        results.push(vec_bytes);
    }

    results
}

/// Size prefixed concatenated byte[] to Vec<Vec<u8>>
pub fn jbytearray_to_vec_bytes<'local>(
    env: &mut JNIEnv<'local>,
    input: JByteArray<'local>,
) -> Vec<Vec<u8>> {
    let input = jbyte_array_to_vec(&env, input);
    if input.len() == 0 {
        return vec![];
    }

    let mut result = Vec::new();

    let mut i = 0;
    while i < input.len() {
        let size_prefix: [u8; 4] = input[i..i + 4]
            .try_into()
            .expect("size prefix must be 4 bytes wide");
        let size_prefix = i32::from_le_bytes(size_prefix) as usize;
        if size_prefix == 0 {
            // filter out empty inner bytes
            i = i + 4;
            continue;
        }

        let inner_input_slice = &input[i + 4..i + 4 + size_prefix];
        result.push(inner_input_slice.to_vec());

        i = i + 4 + size_prefix;
    }

    result
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
