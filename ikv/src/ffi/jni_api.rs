use jni::objects::{JByteArray, JClass, JObject, JString};
use jni::sys::{jbyteArray, jlong, jstring};
use jni::JNIEnv;
use protobuf::Message;

use crate::controller::external_handle;
use crate::controller::index_builder::IndexBuilder;
use crate::ffi::{api, utils};
use crate::proto::generated_proto::common::IKVStoreConfig;
use crate::proto::generated_proto::streaming::IKVDataEvent;

#[no_mangle]
pub extern "system" fn Java_io_inlined_clients_IKVClientJNI_provideHelloWorld<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jstring {
    let output = env
        .new_string(format!("Hello world from Rust!"))
        .expect("Couldn't create java string!");

    output.into_raw()
}

// NOTE: callers must cleanup their working directories
#[no_mangle]
pub extern "system" fn Java_io_inlined_clients_IKVClientJNI_buildIndex<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    config: JByteArray<'local>,
) {
    let config = utils::jbyte_array_to_vec(&env, config).unwrap();
    let ikv_config = IKVStoreConfig::parse_from_bytes(&config).expect("could not read configs");

    // logging setup
    if let Err(e) = crate::utils::logging::configure_logging(&ikv_config) {
        let exception = format!("Cannot initialize logging: {}", e.to_string());
        let _ = env.throw_new("java/lang/RuntimeException", exception);
        return;
    }

    // initialize builder
    let maybe_builder = IndexBuilder::new(&ikv_config);
    if let Err(e) = maybe_builder {
        let exception = format!("Cannot initialize offline index builder: {}", e.to_string());
        let _ = env.throw_new("java/lang/RuntimeException", exception);
        return;
    }

    // build and export
    let index_builder = maybe_builder.unwrap();
    if let Err(e) = index_builder.build_and_export(&ikv_config) {
        let exception = format!("Cannot build offline index: {}", e.to_string());
        let _ = env.throw_new("java/lang/RuntimeException", exception);
        return;
    }

    // close
    if let Err(e) = index_builder.close() {
        let exception = format!(
            "Cannot close index_builder, failed with error: {}",
            e.to_string()
        );
        let _ = env.throw_new("java/lang/RuntimeException", exception);
        return;
    }
}

#[no_mangle]
pub extern "system" fn Java_io_inlined_clients_IKVClientJNI_open<'local>(
    mut env: JNIEnv<'local>,
    class: JClass<'local>,
    config: JByteArray<'local>,
) -> jlong {
    match open(&env, class, config) {
        Ok(handle) => handle,
        Err(e) => {
            let exception = format!("Cannot open reader, failed with error: {}", e.to_string());
            let _ = env.throw_new("java/lang/RuntimeException", exception);
            return 0;
        }
    }
}

fn open<'local>(
    env: &JNIEnv<'local>,
    _class: JClass<'local>,
    config: JByteArray<'local>,
) -> anyhow::Result<jlong> {
    let config = utils::jbyte_array_to_vec(env, config)?;
    let ikv_config = IKVStoreConfig::parse_from_bytes(&config)?;
    api::open_index(&ikv_config)
}

#[no_mangle]
pub extern "system" fn Java_io_inlined_clients_IKVClientJNI_close<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    if let Err(e) = api::close_index(handle) {
        let exception = format!("Cannot close reader, failed with error: {}", e.to_string());
        let _ = env.throw_new("java/lang/RuntimeException", exception);
        return;
    }
}

#[no_mangle]
pub extern "system" fn Java_io_inlined_clients_IKVClientJNI_readField<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    primary_key: JByteArray<'local>,
    field_name: JString<'local>,
) -> jbyteArray {
    let controller = external_handle::from_external_handle(handle);
    let primary_key = utils::jbyte_array_to_vec(&env, primary_key).unwrap();
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
pub extern "system" fn Java_io_inlined_clients_IKVClientJNI_batchReadField<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    primary_keys: JByteArray<'local>,
    field_name: JString<'local>,
) -> jbyteArray {
    let controller = external_handle::from_external_handle(handle);
    let primary_keys = utils::jbyte_array_to_vec(&env, primary_keys).unwrap();
    let primary_keys = utils::unpack_size_prefixed_bytes(&primary_keys);
    let field_name: String = env.get_string(&field_name).unwrap().into();

    let result = controller
        .index_ref()
        .batch_get_field_values(primary_keys, vec![&field_name]);

    // TODO - ensure we don't return batch response larger than i32
    utils::vec_to_jbyte_array(&env, result)
}

#[no_mangle]
pub extern "system" fn Java_io_inlined_clients_IKVClientJNI_processIKVDataEvent<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    ikv_data_event_bytes: JByteArray<'local>,
) {
    let controller = external_handle::from_external_handle(handle);
    let ikv_data_event_bytes = utils::jbyte_array_to_vec(&env, ikv_data_event_bytes).unwrap();

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

    let _ = match controller.writes_processor_ref().process(&ikv_data_event) {
        Ok(_) => jni::errors::Result::Ok(()),
        Err(e) => env.throw_new(
            "java/lang/RuntimeException",
            format!("Write error for IKVDataEvent. Error: {}", e.to_string()),
        ),
    };
}

#[no_mangle]
pub extern "system" fn Java_io_inlined_clients_IKVClientJNI_flushWrites<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    let controller = external_handle::from_external_handle(handle);
    let _ = match controller.index_ref().flush_writes() {
        Ok(_) => jni::errors::Result::Ok(()),
        Err(e) => env.throw_new(
            "java/lang/RuntimeException",
            format!("Flush error for IKVDataEvent. Error: {}", e.to_string()),
        ),
    };
}
