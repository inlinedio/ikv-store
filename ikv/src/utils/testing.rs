// Testing util methods.

use crate::proto::generated_proto::common::{FieldType, FieldValue};

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
