use jni::objects::{JByteArray, JClass, JList, JObject, JObjectArray, JString};
use jni::sys::{jbyteArray, jlong, jstring};
use jni::JNIEnv;

use crate::index::ckv::CKVIndex;
use crate::index::external_handle;
use crate::jni::utils;

const LENGTH: [u8; 4] = [0, 0, 0, 0];

#[no_mangle]
pub extern "system" fn Java_io_inline_clients_IKVClientJNI_provideHelloWorld<'local>(
    mut env: JNIEnv<'local>,
    class: JClass<'local>,
) -> jstring {
    let output = env
        .new_string(format!("Hello world from Rust!"))
        .expect("Couldn't create java string!");

    output.into_raw()
}

#[no_mangle]
pub extern "system" fn Java_io_inline_clients_IKVClientJNI_createNew<'local>(
    mut env: JNIEnv<'local>,
    class: JClass<'local>,
    mount_path: JString<'local>,
    schema_path: JString<'local>,
) -> jlong {
    let mount_path: String = env
        .get_string(&mount_path)
        .expect("Couldn't get mount_path")
        .into();
    let schema_path: String = env
        .get_string(&schema_path)
        .expect("Couldn't get schema_path")
        .into();

    match CKVIndex::new(&mount_path, &schema_path) {
        Ok(index) => return external_handle::to_external_handle(index),
        Err(e) => {
            eprintln!("Cannot create new indes: {e}");
            panic!("")
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_inline_clients_IKVClientJNI_open<'local>(
    mut env: JNIEnv<'local>,
    class: JClass<'local>,
    mount_path: JString<'local>,
) -> jlong {
    let mount_path: String = env
        .get_string(&mount_path)
        .expect("Couldn't get mount_path")
        .into();
    let index = CKVIndex::open(mount_path).unwrap();
    external_handle::to_external_handle(index)
}

#[no_mangle]
pub extern "system" fn Java_io_inline_clients_IKVClientJNI_close<'local>(
    mut env: JNIEnv<'local>,
    class: JClass<'local>,
    index_handle: jlong,
) {
    {
        let index = external_handle::from_external_handle(index_handle);
        index.close();
    }

    external_handle::close_external_handle(index_handle);
}

#[no_mangle]
pub extern "system" fn Java_io_inline_clients_IKVClientJNI_getBytesFieldValue<'local>(
    mut env: JNIEnv<'local>,
    class: JClass<'local>,
    index_handle: jlong,
    document_id: JByteArray<'local>,
    field_name: JString<'local>,
) -> jbyteArray {
    let field_name: String = env
        .get_string(&field_name)
        .expect("Couldn't get field_name")
        .into();
    let index = external_handle::from_external_handle(index_handle);

    let document_id = utils::jbyte_array_to_vec(&env, document_id);

    let field_value = index.get_field_value_by_name(&document_id, &field_name);
    if field_value.is_none() {
        return JObject::null().into_raw();
    }

    let field_value = field_value.unwrap();
    // TODO - assert that field_value cannot exceed i32

    utils::vec_to_jbyte_array(&env, field_value)
}

#[no_mangle]
pub extern "system" fn Java_io_inline_clients_IKVClientJNI_getBatchBytesFieldValueV2<'local>(
    mut env: JNIEnv<'local>,
    class: JClass<'local>,
    index_handle: jlong,
    document_ids: JObject<'local>,
    field_name: JString<'local>,
    results: JObject<'local>,
) {
    let field_name: String = env
        .get_string(&field_name)
        .expect("Couldn't get field_name")
        .into();
    let index = external_handle::from_external_handle(index_handle);

    let mut results = JList::from_env(&mut env, &results).unwrap();

    // get document ids
    let document_id_jlist = JList::from_env(&mut env, &document_ids).unwrap();
    let mut iterator = document_id_jlist.iter(&mut env).unwrap();
    while let Some(obj) = iterator.next(&mut env).unwrap() {
        /*
           Each call to next creates a new local reference.
           To prevent excessive memory usage or overflow error,
           the local reference should be deleted using JNIEnv::delete_local_ref or JNIEnv::auto_local
           before the next loop iteration. Alternatively,
           if the list is known to have a small, predictable size,
           the loop could be wrapped in JNIEnv::with_local_frame to delete all
           of the local references at once.
        */
        let document_id_jbytes: JByteArray = obj.into();
        let document_id = env.convert_byte_array(document_id_jbytes).unwrap();

        let mut result = vec![];
        let _ = index.append_field_value_by_name(&document_id, &field_name, &mut result);

        let result_jbytes = env.byte_array_from_slice(&result).unwrap();
        results.add(&mut env, &result_jbytes).unwrap();
    }
}

#[no_mangle]
pub extern "system" fn Java_io_inline_clients_IKVClientJNI_getBatchBytesFieldValue<'local>(
    mut env: JNIEnv<'local>,
    class: JClass<'local>,
    index_handle: jlong,
    document_ids: JByteArray<'local>,
    field_name: JString<'local>,
) -> jbyteArray {
    let field_name: String = env
        .get_string(&field_name)
        .expect("Couldn't get field_name")
        .into();
    let index = external_handle::from_external_handle(index_handle);

    // size prefixed, concatenated doc-ids
    let document_ids = utils::jbyte_array_to_vec(&env, document_ids);
    if document_ids.len() == 0 {
        return JObject::null().into_raw();
    }

    // allocate result byte array
    // this must be empty!!
    let mut result = vec![];

    let mut i = 0 as usize;
    while i < document_ids.len() {
        // parse document id
        // length of document id (lower endian bytes)
        let document_id_len_bytes: [u8; 4] = document_ids[i..i + 4]
            .try_into()
            .expect("length must be 4 bytes");
        let document_id_len = i32::from_le_bytes(document_id_len_bytes);
        // value of document id
        let start = i + 4;
        let end = start + document_id_len as usize;
        let document_id = &document_ids[start..end];
        i = end;

        // fetch and append to `result` vec

        result.extend_from_slice(&LENGTH);

        let value_len = index.append_field_value_by_name(&document_id, &field_name, &mut result);
        // TODO - assert that field_value cannot exceed i32

        // update w/ correct length
        let end = result.len() - value_len;
        let start = end - 4;
        let value_len_bytes = (value_len as i32).to_le_bytes();
        result[start..end].copy_from_slice(value_len_bytes.as_slice());
        if value_len == 0 {
            // strip last 4 bytes
            let _ = result.drain(start..);
        }
    }

    utils::vec_to_jbyte_array(&env, result)
}

/// DEPRECATED.
#[no_mangle]
pub extern "system" fn Java_io_inline_clients_IKVClientJNI_upsertFieldValue<'local>(
    mut env: JNIEnv<'local>,
    class: JClass<'local>,
    index_handle: jlong,
    primary_key: JByteArray<'local>,
    field_value: JByteArray<'local>,
    field_name: JString<'local>,
) {
    let index = external_handle::from_external_handle(index_handle);
    let primary_key = utils::jbyte_array_to_vec(&env, primary_key);
    let field_value = utils::jbyte_array_to_vec(&env, field_value);
    let field_name: String = env
        .get_string(&field_name)
        .expect("Couldn't get field_name")
        .into();

    index
        .upsert_field_values(&primary_key, vec![field_name], vec![field_value])
        .unwrap();
}

#[no_mangle]
pub extern "system" fn Java_io_inline_clients_IKVClientJNI_upsertFieldValues<'local>(
    mut env: JNIEnv<'local>,
    class: JClass<'local>,
    index_handle: jlong,
    primary_key: JByteArray<'local>,
    field_names: JObject<'local>,
    field_values: JObject<'local>,
) -> jni::errors::Result<()> {
    let index = external_handle::from_external_handle(index_handle);
    let primary_key = utils::jbyte_array_to_vec(&env, primary_key);
    let field_names = utils::jobject_to_vec_strings(&mut env, field_names);
    let field_values: Vec<Vec<u8>> = utils::jobject_to_vec_bytes(&mut env, field_values);

    let result = index.upsert_field_values(&primary_key, field_names, field_values);
    match result {
        Ok(_) => Ok(()),
        Err(e) => env.throw_new("java/lang/RuntimeException", e.to_string()),
    }
}

#[no_mangle]
pub extern "system" fn Java_io_inline_clients_IKVClientJNI_deleteFieldValues<'local>(
    mut env: JNIEnv<'local>,
    class: JClass<'local>,
    index_handle: jlong,
    primary_key: JByteArray<'local>,
    field_names: JObject<'local>,
) -> jni::errors::Result<()> {
    let index = external_handle::from_external_handle(index_handle);
    let primary_key = utils::jbyte_array_to_vec(&env, primary_key);
    let field_names = utils::jobject_to_vec_strings(&mut env, field_names);

    let result = index.delete_field_values(&primary_key, field_names);
    match result {
        Ok(_) => Ok(()),
        Err(e) => env.throw_new("java/lang/RuntimeException", e.to_string()),
    }
}

#[no_mangle]
pub extern "system" fn Java_io_inline_clients_IKVClientJNI_deleteDocument<'local>(
    mut env: JNIEnv<'local>,
    class: JClass<'local>,
    index_handle: jlong,
    primary_key: JByteArray<'local>,
) -> jni::errors::Result<()> {
    let index = external_handle::from_external_handle(index_handle);
    let primary_key = utils::jbyte_array_to_vec(&env, primary_key);

    let result = index.delete_document(&primary_key);
    match result {
        Ok(_) => Ok(()),
        Err(e) => env.throw_new("java/lang/RuntimeException", e.to_string()),
    }
}
