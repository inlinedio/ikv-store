#[derive(Debug, Clone)]
pub struct Field {
    id: u16,
    field_type: FieldType,
}

impl Field {
    pub fn new(id: u16, field_type: FieldType) -> Self {
        Self { id, field_type }
    }

    pub fn id(&self) -> u16 {
        self.id
    }

    /// Length of the field's value - if known.
    pub fn value_len(&self) -> Option<usize> {
        match self.field_type {
            FieldType::I32 => Some(4),
            FieldType::String | FieldType::Bytes => None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum FieldType {
    I32,
    String,
    Bytes,
}

impl TryFrom<String> for FieldType {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "i32" => Ok(FieldType::I32),
            "string" => Ok(FieldType::String),
            "bytes" => Ok(FieldType::Bytes),
            other => Err(format!("Unknown field-type: {} found", other)),
        }
    }
}
