use protobuf::Enum;

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

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut result = Vec::new();

        // serialize values
        result.extend_from_slice((self.name.len() as u64).to_le_bytes().as_slice());
        result.extend_from_slice(self.name.as_bytes());

        result.extend_from_slice(self.id.to_le_bytes().as_slice());

        let field_type_i32 = self.field_type.value();
        result.extend_from_slice(field_type_i32.to_le_bytes().as_slice());

        result
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() == 0 {
            return Err("cannot deser from empty bytes".to_string());
        }

        let offset: usize = 0;

        // deserialize values
        let name_len = u64::from_le_bytes(match bytes[0..8].try_into() {
            Ok(v) => v,
            Err(_) => return Err("cannot deser name".to_string()),
        }) as usize;
        let name = match String::from_utf8(bytes[8..8 + name_len].to_vec()) {
            Ok(v) => v,
            Err(_) => return Err("cannot deser name".to_string()),
        };

        let id = u16::from_le_bytes(match bytes[8 + name_len..8 + name_len + 2].try_into() {
            Ok(v) => v,
            Err(_) => return Err("cannot deser id".to_string()),
        });

        let field_type_i32 = i32::from_le_bytes(
            match bytes[8 + name_len + 2..8 + name_len + 2 + 4].try_into() {
                Ok(v) => v,
                Err(_) => return Err("cannot deser field_type".to_string()),
            },
        );
        let field_type = generated_proto::common::FieldType::from_i32(field_type_i32)
            .ok_or("cannot deser field_type".to_string())?;

        Ok(Self {
            name,
            id,
            field_type,
        })
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
