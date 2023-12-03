use anyhow::bail;

use crate::proto::generated_proto;
use crate::proto::generated_proto::common::{FieldSchema, FieldType};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    name: String,
    id: u16,
    field_type: generated_proto::common::FieldType,
}

impl Field {
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
            generated_proto::common::FieldType::UNKNOWN => unreachable!(),
            generated_proto::common::FieldType::INT32 => Some(4),
            generated_proto::common::FieldType::INT64 => Some(8),
            generated_proto::common::FieldType::FLOAT32 => Some(4),
            generated_proto::common::FieldType::FLOAT64 => Some(8),
            generated_proto::common::FieldType::STRING
            | generated_proto::common::FieldType::BYTES => None,
        }
    }
}

/// Go from proto FieldSchema to Field
impl TryFrom<&FieldSchema> for Field {
    type Error = anyhow::Error;

    fn try_from(field_schema: &FieldSchema) -> anyhow::Result<Self> {
        let name = field_schema.name.to_string();

        let id = field_schema.id;
        if id > u16::MAX as i32 {
            bail!(
                "RangeExhausted - cannot support more than 2^16 fields, rejecting field: {}",
                name
            );
        }

        let field_type = field_schema.fieldType.enum_value_or_default();
        if field_type == FieldType::UNKNOWN {
            bail!("Unknown FieldType, rejecting field: {}", name);
        }

        Ok(Self {
            name,
            id: field_schema.id as u16,
            field_type: field_type.into(),
        })
    }
}

pub struct IndexedValue {
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

impl TryFrom<&generated_proto::common::FieldValue> for IndexedValue {
    type Error = anyhow::Error;

    fn try_from(field_value: &generated_proto::common::FieldValue) -> anyhow::Result<Self> {
        if field_value.fieldType.enum_value_or_default() == FieldType::UNKNOWN {
            bail!("Cannot convert to IndexedValue for UnknownField");
        }

        Ok(Self {
            value: field_value.value.clone(),
        })
    }
}
