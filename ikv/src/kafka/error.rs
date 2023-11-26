use std::{fmt, io::Error};

use rdkafka::error::KafkaError;

use crate::schema::error::SchemaError;

#[derive(Debug, Clone)]
pub enum IKVKafkaError {
    KAFKA_ERROR(KafkaError),
    INDEX_WRITE_ERROR(String),
    STDIO_ERROR(String),
}

impl fmt::Display for IKVKafkaError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            IKVKafkaError::KAFKA_ERROR(e) => write!(f, "{}", e.to_string()),
            IKVKafkaError::INDEX_WRITE_ERROR(e) => write!(f, "{}", e),
            IKVKafkaError::STDIO_ERROR(e) => write!(f, "{}", e),
        }
    }
}

impl From<SchemaError> for IKVKafkaError {
    fn from(value: SchemaError) -> Self {
        IKVKafkaError::INDEX_WRITE_ERROR(value.to_string())
    }
}

impl From<KafkaError> for IKVKafkaError {
    fn from(value: KafkaError) -> Self {
        IKVKafkaError::KAFKA_ERROR(value)
    }
}

impl From<std::io::Error> for IKVKafkaError {
    fn from(value: std::io::Error) -> Self {
        IKVKafkaError::STDIO_ERROR(value.to_string())
    }
}
