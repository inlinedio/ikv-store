use core::fmt;

use crate::schema::error::SchemaError;

pub enum IndexError {
    IllegalArguments(String),
}

impl fmt::Display for IndexError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        todo!()
    }
}

impl From<SchemaError> for IndexError {
    fn from(value: SchemaError) -> Self {
        IndexError::IllegalArguments(format!(
            "Unsupported field-type, schema-error: {}",
            value.to_string()
        ))
    }
}

impl From<std::io::Error> for IndexError {
    fn from(value: std::io::Error) -> Self {
        todo!()
    }
}
