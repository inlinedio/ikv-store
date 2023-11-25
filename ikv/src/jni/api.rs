use jni::objects::{JByteArray, JClass, JObject, JString};
use jni::sys::{jbyteArray, jlong, jstring};
use jni::JNIEnv;
use protobuf::Message;

use crate::controller::external_handle;
use crate::controller::main::Controller;
use crate::jni::utils;
use crate::proto::generated_proto::common::IKVStoreConfig;

#[no_mangle]
pub extern "system" fn Java_io_inline_clients_IKVClientJNI_provideHelloWorld<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jstring {
    let output = env
        .new_string(format!("Hello world from Rust!"))
        .expect("Couldn't create java string!");

    output.into_raw()
}

#[no_mangle]
pub extern "system" fn Java_io_inline_clients_IKVClientJNI_open<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    config: JByteArray<'local>,
) -> jlong {
    let config = utils::jbyte_array_to_vec(&env, config);
    let ikv_config = IKVStoreConfig::parse_from_bytes(&config).expect("could not read configs");

    let maybe_controller = Controller::open(ikv_config);
    if let Err(e) = maybe_controller {
        env.throw_new("java/lang/RuntimeException", e);
        return 0;
    }

    let controller = maybe_controller.unwrap();
    external_handle::to_external_handle(controller)
}

#[no_mangle]
pub extern "system" fn Java_io_inline_clients_IKVClientJNI_close<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    let boxed_controller = external_handle::to_box(handle);
    boxed_controller.close();
}

#[no_mangle]
pub extern "system" fn Java_io_inline_clients_IKVClientJNI_readField<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    primary_key: JByteArray<'local>,
    field_name: JString<'local>,
) -> jbyteArray {
    let controller = external_handle::from_external_handle(handle);
    let primary_key = utils::jbyte_array_to_vec(&env, primary_key);
    let field_name: String = env.get_string(&field_name).unwrap().into();

    let maybe_field_value = controller
        .index_ref()
        .get_field_value(&primary_key, &field_name);
    if maybe_field_value.is_none() {
        return JObject::null().into_raw();
    }

    // TODO - ensure we don't upsert values larger than i32
    utils::vec_to_jbyte_array(&env, maybe_field_value.unwrap())
}

#[no_mangle]
pub extern "system" fn Java_io_inline_clients_IKVClientJNI_readFields<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    primary_key: JByteArray<'local>,
    field_names: JObject<'local>,
) -> jbyteArray {
    let controller = external_handle::from_external_handle(handle);
    let primary_key = utils::jbyte_array_to_vec(&env, primary_key);
    let field_names = utils::jlist_to_vec_strings(&mut env, field_names);

    let result = controller
        .index_ref()
        .batch_get_field_values(vec![primary_key], field_names);

    // TODO - ensure we don't return batch response larger than i32
    utils::vec_to_jbyte_array(&env, result)
}

#[no_mangle]
pub extern "system" fn Java_io_inline_clients_IKVClientJNI_batchReadField<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    primary_keys: JByteArray<'local>,
    field_name: JString<'local>,
) -> jbyteArray {
    let controller = external_handle::from_external_handle(handle);
    let primary_keys = utils::jbytearray_to_vec_bytes(&mut env, primary_keys);
    let field_name: String = env.get_string(&field_name).unwrap().into();

    let result = controller
        .index_ref()
        .batch_get_field_values(primary_keys, vec![field_name]);

    // TODO - ensure we don't return batch response larger than i32
    utils::vec_to_jbyte_array(&env, result)
}

#[no_mangle]
pub extern "system" fn Java_io_inline_clients_IKVClientJNI_batchReadFields<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    primary_keys: JObject<'local>,
    field_names: JObject<'local>,
) -> jbyteArray {
    let controller = external_handle::from_external_handle(handle);
    let primary_keys = utils::jlist_to_vec_bytes(&mut env, primary_keys);
    let field_names = utils::jlist_to_vec_strings(&mut env, field_names);

    let result = controller
        .index_ref()
        .batch_get_field_values(primary_keys, field_names);

    // TODO - ensure we don't return batch response larger than i32
    utils::vec_to_jbyte_array(&env, result)
}

#[no_mangle]
pub extern "system" fn Java_io_inline_clients_IKVClientJNI_upsertFieldValues<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    primary_key: JByteArray<'local>,
    field_names: JObject<'local>,
    field_values: JObject<'local>,
) {
    let controller = external_handle::from_external_handle(handle);
    let primary_key = utils::jbyte_array_to_vec(&env, primary_key);
    let field_names = utils::jlist_to_vec_strings(&mut env, field_names);
    let field_values: Vec<Vec<u8>> = utils::jlist_to_vec_bytes(&mut env, field_values);

    let _ =
        match controller
            .index_ref()
            .jni_upsert_field_values(primary_key, field_names, field_values)
        {
            Ok(_) => jni::errors::Result::Ok(()),
            Err(err) => env.throw_new("java/lang/RuntimeException", err.to_string()),
        };
}

#[no_mangle]
pub extern "system" fn Java_io_inline_clients_IKVClientJNI_deleteFieldValues<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    primary_key: JByteArray<'local>,
    field_names: JObject<'local>,
) {
    let controller = external_handle::from_external_handle(handle);
    let primary_key = utils::jbyte_array_to_vec(&env, primary_key);
    let field_names = utils::jlist_to_vec_strings(&mut env, field_names);

    let _ = controller
        .index_ref()
        .legacy_delete_field_values(&primary_key, field_names);
}

#[no_mangle]
pub extern "system" fn Java_io_inline_clients_IKVClientJNI_deleteDocument<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    primary_key: JByteArray<'local>,
) {
    let controller = external_handle::from_external_handle(handle);
    let primary_key = utils::jbyte_array_to_vec(&env, primary_key);

    let _ = controller.index_ref().legacy_delete_document(&primary_key);
}
