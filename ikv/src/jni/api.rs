use jni::objects::{JByteArray, JClass, JObject, JString};
use jni::sys::{jbyteArray, jlong, jstring};
use jni::JNIEnv;

use crate::index::ckv::CKVIndex;
use crate::index::external_handle;
use crate::jni::utils;

#[no_mangle]
pub extern "system" fn Java_io_inline_IKVClientJNI_provideHelloWorld<'local>(
    mut env: JNIEnv<'local>,
    class: JClass<'local>,
) -> jstring {
    let output = env
        .new_string(format!("Hello world from Rust!"))
        .expect("Couldn't create java string!");

    output.into_raw()
}

#[no_mangle]
pub extern "system" fn Java_io_inline_IKVClientJNI_createNew<'local>(
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
    let index = CKVIndex::new(mount_path, &schema_path).unwrap();
    external_handle::to_external_handle(index)
}

#[no_mangle]
pub extern "system" fn Java_io_inline_IKVClientJNI_open<'local>(
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
pub extern "system" fn Java_io_inline_IKVClientJNI_close<'local>(
    mut env: JNIEnv<'local>,
    class: JClass<'local>,
    index_handle: jlong,
) {
    external_handle::close_external_handle(index_handle);
}

#[no_mangle]
pub extern "system" fn Java_io_inline_IKVClientJNI_getFieldValue<'local>(
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
pub extern "system" fn Java_io_inline_IKVClientJNI_upsertFieldValue<'local>(
    mut env: JNIEnv<'local>,
    class: JClass<'local>,
    index_handle: jlong,
    document_id: JByteArray<'local>,
    field_value: JByteArray<'local>,
    field_name: JString<'local>,
) {
    let index = external_handle::from_external_handle(index_handle);
    let document_id = utils::jbyte_array_to_vec(&env, document_id);
    let field_value = utils::jbyte_array_to_vec(&env, field_value);
    let field_name: String = env
        .get_string(&field_name)
        .expect("Couldn't get field_name")
        .into();

    index
        .upsert_field_value_by_name(&document_id, &field_value, &field_name)
        .unwrap();
}
