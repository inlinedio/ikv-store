use jni::objects::{JByteArray, JClass, JString};
use jni::sys::{jbyteArray, jlong, jstring};
use jni::JNIEnv;

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
    todo!()
}

#[no_mangle]
pub extern "system" fn Java_io_inline_IKVClientJNI_open<'local>(
    mut env: JNIEnv<'local>,
    class: JClass<'local>,
    mount_path: JString<'local>,
) -> jlong {
    todo!()
}

#[no_mangle]
pub extern "system" fn Java_io_inline_IKVClientJNI_close<'local>(
    mut env: JNIEnv<'local>,
    class: JClass<'local>,
    index_handle: jlong,
) {
    todo!()
}

#[no_mangle]
pub extern "system" fn Java_io_inline_IKVClientJNI_getFieldValue<'local>(
    mut env: JNIEnv<'local>,
    class: JClass<'local>,
    index_handle: jlong,
    document_id: JByteArray<'local>,
    field_name: JString<'local>,
) -> jbyteArray {
    todo!()
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
    todo!()
}
