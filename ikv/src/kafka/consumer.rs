use protobuf::Message;
use rdkafka::{
    consumer::{CommitMode, Consumer, ConsumerContext, StreamConsumer},
    error::KafkaResult,
    ClientContext, TopicPartitionList,
};

use crate::proto::generated_proto::{common::IKVStoreConfig, streaming::IKVDataEvent};

use super::processor::WritesProcessor;

pub struct IKVKafkaConsumer {
    kafka_consumer: StreamConsumer<IKVKafkaConsumerContext>,
    processor: WritesProcessor,
}

impl IKVKafkaConsumer {
    /// Create a new processor.
    pub fn new(config: &IKVStoreConfig, processor: WritesProcessor) -> Self {
        todo!()
    }

    pub fn run_in_background(&self) -> KafkaResult<()> {
        // TODO: add correct topic and partition
        let mut topic_partition = TopicPartitionList::new();
        topic_partition.add_partition("foo_topic", 0);

        self.kafka_consumer.assign(&topic_partition)?;

        Ok(())
    }

    async fn run_blocking(&self) {
        loop {
            match self.kafka_consumer.recv().await {
                Err(e) => {
                    // TODO: log unprocessed event and continue?
                }
                Ok(message) => {
                    if let Some(bytes) = rdkafka::Message::payload(&message) {
                        match IKVDataEvent::parse_from_bytes(bytes) {
                            Ok(event) => {
                                // TODO: handle errors
                                let _ = self.processor.process(&event);
                            }
                            Err(e) => {
                                // TODO: log deserialization errors
                            }
                        }
                    }

                    // TODO: handle error while committing
                    let _ = self
                        .kafka_consumer
                        .commit_message(&message, CommitMode::Sync);
                }
            };
        }
    }

    pub fn stop(&self) {
        todo!()
    }
}

pub struct IKVKafkaConsumerContext;

impl ClientContext for IKVKafkaConsumerContext {}
impl ConsumerContext for IKVKafkaConsumerContext {
    fn commit_callback(&self, result: KafkaResult<()>, offsets: &TopicPartitionList) {
        // Store offsets on disk.
        todo!()
    }
}
