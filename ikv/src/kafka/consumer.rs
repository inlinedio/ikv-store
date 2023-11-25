use std::{sync::Arc, time::Duration};

use protobuf::Message;
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{CommitMode, Consumer, ConsumerContext, StreamConsumer},
    error::KafkaResult,
    ClientConfig, ClientContext, TopicPartitionList,
};
use tokio::runtime::{Builder, Runtime};

use crate::proto::generated_proto::{common::IKVStoreConfig, streaming::IKVDataEvent};

use super::{error::IKVKafkaError, processor::WritesProcessor};

pub struct IKVKafkaConsumer {
    is_running: bool,
    runtime: Runtime,
    consumer: StreamConsumer<IKVKafkaConsumerContext>,
    processor: Arc<WritesProcessor>,
}

impl IKVKafkaConsumer {
    /// Create a new processor.
    pub fn new(
        config: &IKVStoreConfig,
        processor: Arc<WritesProcessor>,
    ) -> Result<Self, IKVKafkaError> {
        let kafka_consumer_bootstrap_server = config
            .stringConfigs
            .get("kafka_consumer_bootstrap_server")
            .ok_or(IKVKafkaError::KAFKA_ERROR(
                rdkafka::error::KafkaError::ClientCreation(
                    "kafka_consumer_bootstrap_server is a required config".to_string(),
                ),
            ))?;

        // TODO: we might need SSL access
        // Ref: https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html
        let consumer: StreamConsumer<IKVKafkaConsumerContext> = ClientConfig::new()
            //.set("group.id", group_id) // we don't use offset management or automatic partition assignment
            .set("bootstrap.servers", kafka_consumer_bootstrap_server)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "30000000") // ~1year
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set_log_level(RDKafkaLogLevel::Debug)
            .create_with_context(IKVKafkaConsumerContext)?;

        // assign topic and parition
        let topic = config
            .stringConfigs
            .get("kafka_topic")
            .ok_or(IKVKafkaError::KAFKA_ERROR(
                rdkafka::error::KafkaError::ClientCreation(
                    "kafka_topic is a required config".to_string(),
                ),
            ))?;
        let partition =
            config
                .numericConfigs
                .get("kafka_partition")
                .ok_or(IKVKafkaError::KAFKA_ERROR(
                    rdkafka::error::KafkaError::ClientCreation(
                        "kafka_partition is a required config".to_string(),
                    ),
                ))?;
        let partition = if (*partition > i32::MAX as i64) || (*partition < 0) {
            return Err(IKVKafkaError::KAFKA_ERROR(
                rdkafka::error::KafkaError::ClientCreation("kafka_partition bad value".to_string()),
            ));
        } else {
            *partition as i32
        };

        let mut topic_partition = TopicPartitionList::new();
        topic_partition.add_partition(&topic, partition);
        consumer.assign(&topic_partition)?;

        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("kafka-consumer-thread")
            .build()?;

        Ok(IKVKafkaConsumer {
            is_running: false,
            consumer,
            processor,
            runtime,
        })
    }

    pub fn run_in_background(&mut self) -> KafkaResult<()> {
        /*if !self.is_running {
            self.is_running = true;
            self.runtime.spawn(self.run_blocking());
        }
        Ok(())*/
        todo!()
    }

    async fn run_blocking(&self) {
        loop {
            if !self.is_running {
                break;
            }

            match self.consumer.recv().await {
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
                    let _ = self.consumer.commit_message(&message, CommitMode::Sync);
                }
            };
        }
    }

    pub fn stop(mut self) {
        self.is_running = false;
        self.runtime.shutdown_timeout(Duration::from_secs(60));
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
