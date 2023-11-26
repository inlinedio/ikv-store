use core::fmt;

use crate::schema::error::SchemaError;

pub enum IndexError {
    IllegalArguments(String),
    SystemError(String),
}

impl fmt::Display for IndexError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            IndexError::IllegalArguments(s) => write!(f, "{}", s),
            IndexError::SystemError(s) => write!(f, "{}", s),
        }
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
        IndexError::SystemError(value.to_string())
    }
}
