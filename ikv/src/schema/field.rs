use crate::proto::generated_proto;
use crate::proto::generated_proto::common::FieldSchema;

use super::error::SchemaError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    name: String,
    id: u16,
    field_type: FieldType,
}

impl Field {
    pub fn new(name: String, id: u16, field_type: FieldType) -> Self {
        Self {
            name,
            id,
            field_type,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn id(&self) -> u16 {
        self.id
    }

    /// Length of the field's value - if known (ie fixed width).
    pub fn value_len(&self) -> Option<usize> {
        match self.field_type {
            FieldType::I32 => Some(4),
            FieldType::I64 => Some(8),
            FieldType::F32 => Some(4),
            FieldType::F64 => Some(8),
            FieldType::String | FieldType::Bytes => None,
        }
    }
}

impl TryFrom<&FieldSchema> for Field {
    type Error = SchemaError;

    fn try_from(field_schema: &FieldSchema) -> Result<Self, SchemaError> {
        let id = field_schema.id;
        if id > u16::MAX as i32 {
            return Err(SchemaError::RangeExhausted);
        }

        let field_type = match field_schema.fieldType.enum_value() {
            Ok(ft) => ft,
            Err(_) => {
                // new unknown value
                return Err(SchemaError::UnsupportedField);
            }
        };

        Ok(Self {
            name: field_schema.name.to_string(),
            id: field_schema.id as u16,
            field_type: field_type.into(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldType {
    I32,
    I64,
    F32,
    F64,
    String,
    Bytes,
}

impl From<generated_proto::common::FieldType> for FieldType {
    fn from(ft: generated_proto::common::FieldType) -> Self {
        match ft {
            generated_proto::common::FieldType::INT32 => FieldType::I32,
            generated_proto::common::FieldType::INT64 => FieldType::I64,
            generated_proto::common::FieldType::FLOAT32 => FieldType::F32,
            generated_proto::common::FieldType::FLOAT64 => FieldType::F64,
            generated_proto::common::FieldType::STRING => FieldType::String,
            generated_proto::common::FieldType::BYTES => FieldType::Bytes,
        }
    }
}

impl TryFrom<String> for FieldType {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "i32" => Ok(FieldType::I32),
            "i64" => Ok(FieldType::I64),
            "f32" => Ok(FieldType::F32),
            "f64" => Ok(FieldType::F64),
            "string" => Ok(FieldType::String),
            "bytes" => Ok(FieldType::Bytes),
            other => Err(format!("Unknown field-type: {} found", other)),
        }
    }
}

pub struct IndexedValue {
    _field_type: FieldType,
    value: Vec<u8>,
}

impl IndexedValue {
    pub fn serialized_ref(&self) -> &[u8] {
        &self.value
    }

    pub fn serialize(self) -> Vec<u8> {
        self.value
    }

    pub fn len(&self) -> usize {
        self.value.len()
    }
}

impl TryFrom<&generated_proto::services::FieldValue> for IndexedValue {
    type Error = SchemaError;

    fn try_from(value: &generated_proto::services::FieldValue) -> Result<Self, Self::Error> {
        if value.Value.is_none() {
            // new member in field-value union
            return Err(SchemaError::UnsupportedField);
        }

        let field_value = match value.Value.as_ref().unwrap() {
            generated_proto::services::field_value::Value::Int32Value(v) => Self {
                _field_type: FieldType::I32,
                value: v.to_le_bytes().to_vec(),
            },
            generated_proto::services::field_value::Value::Int64Value(v) => Self {
                _field_type: FieldType::I64,
                value: v.to_le_bytes().to_vec(),
            },
            generated_proto::services::field_value::Value::Float32Value(v) => Self {
                _field_type: FieldType::F32,
                value: v.to_le_bytes().to_vec(),
            },
            generated_proto::services::field_value::Value::Float64Value(v) => Self {
                _field_type: FieldType::F64,
                value: v.to_le_bytes().to_vec(),
            },
            generated_proto::services::field_value::Value::StringValue(v) => Self {
                _field_type: FieldType::F64,
                value: v.clone().into_bytes(),
            },
            generated_proto::services::field_value::Value::BytesValue(v) => Self {
                _field_type: FieldType::F64,
                value: v.clone(),
            },
        };

        Ok(field_value)
    }
}
