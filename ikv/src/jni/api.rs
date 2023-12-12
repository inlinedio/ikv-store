use jni::objects::{JByteArray, JClass, JObject, JString};
use jni::sys::{jbyteArray, jlong, jstring};
use jni::JNIEnv;
use protobuf::Message;

use crate::controller::external_handle;
use crate::controller::index_builder::IndexBuilder;
use crate::controller::main::Controller;
use crate::jni::utils;
use crate::proto::generated_proto::common::IKVStoreConfig;
use crate::proto::generated_proto::streaming::IKVDataEvent;

#[no_mangle]
pub extern "system" fn Java_io_inline_clients_internal_IKVClientJNI_provideHelloWorld<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jstring {
    let output = env
        .new_string(format!("Hello world from Rust!"))
        .expect("Couldn't create java string!");

    output.into_raw()
}

#[no_mangle]
pub extern "system" fn Java_io_inline_clients_internal_IKVClientJNI_buildIndex<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    config: JByteArray<'local>,
) {
    let config = utils::jbyte_array_to_vec(&env, config);
    let ikv_config = IKVStoreConfig::parse_from_bytes(&config).expect("could not read configs");

    let maybe_builder = IndexBuilder::new(ikv_config);
    if let Err(e) = maybe_builder {
        let exception = format!("Cannot initialize offline index builder: {}", e.to_string());
        let _ = env.throw_new("java/lang/RuntimeException", exception);
        return;
    }

    let index_builder = maybe_builder.unwrap();
    if let Err(e) = index_builder.build_and_export() {
        let exception = format!("Cannot build offline index: {}", e.to_string());
        let _ = env.throw_new("java/lang/RuntimeException", exception);
        return;
    }
}

#[no_mangle]
pub extern "system" fn Java_io_inline_clients_internal_IKVClientJNI_open<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    config: JByteArray<'local>,
) -> jlong {
    let config = utils::jbyte_array_to_vec(&env, config);
    let ikv_config = IKVStoreConfig::parse_from_bytes(&config).expect("could not read configs");

    let maybe_controller = Controller::open(ikv_config);
    if let Err(e) = maybe_controller {
        let _ = env.throw_new("java/lang/RuntimeException", e.to_string());
        return 0;
    }

    let controller = maybe_controller.unwrap();
    external_handle::to_external_handle(controller)
}

#[no_mangle]
pub extern "system" fn Java_io_inline_clients_internal_IKVClientJNI_close<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    let boxed_controller = external_handle::to_box(handle);
    boxed_controller.close();
}

#[no_mangle]
pub extern "system" fn Java_io_inline_clients_internal_IKVClientJNI_readField<'local>(
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
pub extern "system" fn Java_io_inline_clients_internal_IKVClientJNI_batchReadField<'local>(
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
pub extern "system" fn Java_io_inline_clients_internal_IKVClientJNI_processIKVDataEvent<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    ikv_data_event_bytes: JByteArray<'local>,
) {
    let controller = external_handle::from_external_handle(handle);
    let ikv_data_event_bytes = utils::jbyte_array_to_vec(&env, ikv_data_event_bytes);

    let maybe_ikv_data_event = IKVDataEvent::parse_from_bytes(&ikv_data_event_bytes);
    if let Err(e) = maybe_ikv_data_event {
        let _ = env.throw_new(
            "java/lang/RuntimeException",
            format!(
                "Cannot deserialize to IKVDataEvent. Error: {}",
                e.to_string()
            ),
        );
        return;
    }

    // Write to index
    let ikv_data_event = maybe_ikv_data_event.unwrap();

    let _ = match controller
        .writes_processor_ref()
        .process_or_throw(&ikv_data_event)
    {
        Ok(_) => jni::errors::Result::Ok(()),
        Err(e) => env.throw_new(
            "java/lang/RuntimeException",
            format!("Write error for IKVDataEvent. Error: {}", e.to_string()),
        ),
    };
}
