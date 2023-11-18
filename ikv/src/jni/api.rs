use jni::objects::{JByteArray, JClass, JObject, JString};
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
pub extern "system" fn Java_io_inline_clients_IKVClientJNI_readField<'local>(
    mut env: JNIEnv<'local>,
    class: JClass<'local>,
    index_handle: jlong,
    primary_key: JByteArray<'local>,
    field_name: JString<'local>,
) -> jbyteArray {
    let index = external_handle::from_external_handle(index_handle);
    let primary_key = utils::jbyte_array_to_vec(&env, primary_key);
    let field_name: String = env.get_string(&field_name).unwrap().into();

    let maybe_field_value = index.get_field_value(&primary_key, &field_name);
    if maybe_field_value.is_none() {
        return JObject::null().into_raw();
    }

    // TODO - ensure we don't upsert values larger than i32
    utils::vec_to_jbyte_array(&env, maybe_field_value.unwrap())
}

#[no_mangle]
pub extern "system" fn Java_io_inline_clients_IKVClientJNI_readFields<'local>(
    mut env: JNIEnv<'local>,
    class: JClass<'local>,
    index_handle: jlong,
    primary_key: JByteArray<'local>,
    field_names: JObject<'local>,
) -> jbyteArray {
    let index = external_handle::from_external_handle(index_handle);
    let primary_key = utils::jbyte_array_to_vec(&env, primary_key);
    let field_names = utils::jlist_to_vec_strings(&mut env, field_names);

    let result = index.batch_get_field_values(vec![primary_key], field_names);

    // TODO - ensure we don't return batch response larger than i32
    utils::vec_to_jbyte_array(&env, result)
}

#[no_mangle]
pub extern "system" fn Java_io_inline_clients_IKVClientJNI_batchReadField<'local>(
    mut env: JNIEnv<'local>,
    class: JClass<'local>,
    index_handle: jlong,
    primary_keys: JByteArray<'local>,
    field_name: JString<'local>,
) -> jbyteArray {
    let index = external_handle::from_external_handle(index_handle);
    let primary_keys = utils::jbytearray_to_vec_bytes(&mut env, primary_keys);
    let field_name: String = env.get_string(&field_name).unwrap().into();

    let result = index.batch_get_field_values(primary_keys, vec![field_name]);

    // TODO - ensure we don't return batch response larger than i32
    utils::vec_to_jbyte_array(&env, result)
}

#[no_mangle]
pub extern "system" fn Java_io_inline_clients_IKVClientJNI_batchReadFields<'local>(
    mut env: JNIEnv<'local>,
    class: JClass<'local>,
    index_handle: jlong,
    primary_keys: JObject<'local>,
    field_names: JObject<'local>,
) -> jbyteArray {
    let index = external_handle::from_external_handle(index_handle);
    let primary_keys = utils::jlist_to_vec_bytes(&mut env, primary_keys);
    let field_names = utils::jlist_to_vec_strings(&mut env, field_names);

    let result = index.batch_get_field_values(primary_keys, field_names);

    // TODO - ensure we don't return batch response larger than i32
    utils::vec_to_jbyte_array(&env, result)
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
    let field_names = utils::jlist_to_vec_strings(&mut env, field_names);
    let field_values: Vec<Vec<u8>> = utils::jlist_to_vec_bytes(&mut env, field_values);

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
    let field_names = utils::jlist_to_vec_strings(&mut env, field_names);

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
