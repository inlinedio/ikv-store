use std::sync::RwLock;

use rdkafka::{consumer::ConsumerContext, error::KafkaResult, ClientContext, TopicPartitionList};

use super::offset_store::OffsetStore;

pub struct IKVKafkaConsumerContext {
    offset_store: RwLock<OffsetStore>,
}

impl IKVKafkaConsumerContext {
    pub fn new(offset_store: OffsetStore) -> Self {
        Self {
            offset_store: RwLock::new(offset_store),
        }
    }
}

impl ClientContext for IKVKafkaConsumerContext {}

impl ConsumerContext for IKVKafkaConsumerContext {
    fn commit_callback(&self, _result: KafkaResult<()>, offsets: &TopicPartitionList) {
        let mut offset_store = self.offset_store.write().unwrap();
        offset_store.write_all_offsets(offsets).unwrap(); // TODO: no unwrap! the stream must stop!!
    }
}
