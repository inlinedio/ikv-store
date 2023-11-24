use rdkafka::error::KafkaError;

use crate::schema::error::SchemaError;

#[derive(Debug, Clone)]
pub enum IKVKafkaError {
    KAFKA_ERROR(KafkaError),
    INDEX_WRITE_ERROR(String),
}

impl From<SchemaError> for IKVKafkaError {
    fn from(value: SchemaError) -> Self {
        IKVKafkaError::INDEX_WRITE_ERROR(value.to_string())
    }
}
