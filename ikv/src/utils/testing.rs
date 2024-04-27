// Testing util methods.

use std::collections::HashMap;

use crate::proto::generated_proto::common::{FieldType, FieldValue, IKVStoreConfig};

pub fn string_to_field_value(value: &str) -> FieldValue {
    let mut fv = FieldValue::new();
    fv.fieldType = FieldType::STRING.into();
    fv.value = value.as_bytes().to_vec();
    fv
}

pub fn bytes_to_field_value(value: &[u8]) -> FieldValue {
    let mut fv = FieldValue::new();
    fv.fieldType = FieldType::BYTES.into();
    fv.value = value.to_vec();
    fv
}

pub fn i32_to_field_value(value: i32) -> FieldValue {
    let mut fv = FieldValue::new();
    fv.fieldType = FieldType::INT32.into();
    fv.value = value.to_le_bytes().to_vec();
    fv
}

pub const PRIMARY_KEY_FIELD_NAME: &str = "field0";
pub const DOCFIELD1: &str = "field1";
pub const DOCFIELD2: &str = "field2";
pub const DOCFIELD3: &str = "field3";

/// Dummy document generator with 1 primary key and 3 fields
/// {"field0": pkey-as-str, "field1": str, "field2": bytes, "field3": int}
pub fn create_document(primary_key: u32) -> HashMap<String, FieldValue> {
    let mut document = HashMap::new();

    // string
    document.insert(
        "field0".to_string(),
        string_to_field_value(&format!("field0:{}", primary_key)),
    );

    // string
    document.insert(
        "field1".to_string(),
        string_to_field_value(&format!("field1:{}", primary_key)),
    );

    // bytes
    document.insert(
        "field2".to_string(),
        bytes_to_field_value(&format!("field2:{}", primary_key).as_bytes()),
    );

    // int
    document.insert("field3".to_string(), i32_to_field_value(primary_key as i32));

    document
}

pub fn setup_index_cfg(mount_dir: &str) -> IKVStoreConfig {
    let mut ikv_config = IKVStoreConfig::new();
    ikv_config.stringConfigs = HashMap::new();
    ikv_config
        .stringConfigs
        .insert("mount_directory".to_string(), mount_dir.to_string());
    ikv_config
        .stringConfigs
        .insert("store_name".to_string(), "ckv_test_store".to_string());
    ikv_config
        .stringConfigs
        .insert("primary_key_field_name".to_string(), "field0".to_string());

    ikv_config.intConfigs = HashMap::new();
    ikv_config.intConfigs.insert("partition".to_string(), 0);

    ikv_config
}
