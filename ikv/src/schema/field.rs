use crate::proto::generated_proto;
use crate::proto::generated_proto::common::FieldSchema;

use super::error::SchemaError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    name: String,
    id: u16,
    field_type: generated_proto::common::FieldType,
}

impl Field {
    pub fn new(name: String, id: u16, field_type: generated_proto::common::FieldType) -> Self {
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

    pub fn field_type(&self) -> &generated_proto::common::FieldType {
        &self.field_type
    }

    /// Length of the field's value - if known (ie fixed width).
    pub fn value_len(&self) -> Option<usize> {
        match self.field_type {
            generated_proto::common::FieldType::INT32 => Some(4),
            generated_proto::common::FieldType::INT64 => Some(8),
            generated_proto::common::FieldType::FLOAT32 => Some(4),
            generated_proto::common::FieldType::FLOAT64 => Some(8),
            generated_proto::common::FieldType::STRING
            | generated_proto::common::FieldType::BYTES => None,
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

pub struct IndexedValue {
    _field_type: generated_proto::common::FieldType,
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
                _field_type: generated_proto::common::FieldType::INT32,
                value: v.to_le_bytes().to_vec(),
            },
            generated_proto::services::field_value::Value::Int64Value(v) => Self {
                _field_type: generated_proto::common::FieldType::INT64,
                value: v.to_le_bytes().to_vec(),
            },
            generated_proto::services::field_value::Value::Float32Value(v) => Self {
                _field_type: generated_proto::common::FieldType::FLOAT32,
                value: v.to_le_bytes().to_vec(),
            },
            generated_proto::services::field_value::Value::Float64Value(v) => Self {
                _field_type: generated_proto::common::FieldType::FLOAT64,
                value: v.to_le_bytes().to_vec(),
            },
            generated_proto::services::field_value::Value::StringValue(v) => Self {
                _field_type: generated_proto::common::FieldType::STRING,
                value: v.clone().into_bytes(),
            },
            generated_proto::services::field_value::Value::BytesValue(v) => Self {
                _field_type: generated_proto::common::FieldType::BYTES,
                value: v.clone(),
            },
        };

        Ok(field_value)
    }
}
