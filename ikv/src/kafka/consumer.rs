use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
use std::time::Duration;
use std::{thread, time};

use protobuf::Message;
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{CommitMode, Consumer, ConsumerContext, StreamConsumer},
    error::KafkaResult,
    ClientConfig, ClientContext, TopicPartitionList,
};
use tokio::runtime::{Builder, Runtime};
use tokio_util::sync::CancellationToken;

use crate::proto::generated_proto::{common::IKVStoreConfig, streaming::IKVDataEvent};

use super::{error::IKVKafkaError, processor::WritesProcessor};

#[cfg(test)]
#[path = "consumer_test.rs"]
mod consumer_test;

pub struct IKVKafkaConsumer {
    tokio_runtime: Runtime,
    processor: Arc<WritesProcessor>,

    // consumer thread
    start_signal: (Sender<i32>, Receiver<i32>),
    cancellation_token: CancellationToken,

    // Consumer configuration - created in constructor
    client_config: ClientConfig,
    topic_partition: TopicPartitionList,
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
        let client_config = ClientConfig::new()
            .set("group.id", "no op") // we don't use offset management or automatic partition assignment
            .set("bootstrap.servers", kafka_consumer_bootstrap_server)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "3600000")
            .set("max.poll.interval.ms", "3600000")
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("enable.partition.eof", "false")
            .set_log_level(RDKafkaLogLevel::Emerg)
            .clone();

        // topic and parition
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
        topic_partition
            .add_partition_offset(&topic, partition, rdkafka::Offset::Offset(0))
            .unwrap();

        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("kafka-consumer-thread")
            .enable_time()
            .build()?;

        Ok(IKVKafkaConsumer {
            tokio_runtime: runtime,
            processor: processor,
            start_signal: mpsc::channel(),
            cancellation_token: CancellationToken::new(),
            client_config: client_config,
            topic_partition: topic_partition,
        })
    }

    pub fn run_in_background(&self) -> Result<(), IKVKafkaError> {
        // start consumer thread
        self.tokio_runtime.spawn(IKVKafkaConsumer::run(
            self.processor.clone(),
            self.start_signal.0.clone(),
            self.cancellation_token.clone(),
            self.client_config.clone(),
            self.topic_partition.clone(),
        ));

        // wait for status - kafka creation
        if let Ok(signal) = self.start_signal.1.recv() {
            if signal == 0 {
                thread::sleep(time::Duration::from_millis(10000)); // 10s sleep
                return Ok(());
            } else if signal == -1 {
                return Err(IKVKafkaError::KAFKA_ERROR(
                    rdkafka::error::KafkaError::ClientCreation("cannot create client".to_string()),
                ));
            } else {
                unreachable!()
            }
        };

        Ok(())
    }

    // blocking run.
    async fn run(
        processor: Arc<WritesProcessor>,
        start_signal: Sender<i32>,
        cancellation_token: CancellationToken,
        client_config: ClientConfig,
        topic_partition: TopicPartitionList,
    ) {
        let consumer: StreamConsumer<IKVKafkaConsumerContext> =
            match client_config.create_with_context(IKVKafkaConsumerContext) {
                Ok(consumer) => consumer,
                Err(e) => {
                    println!("{}", format!("Cannot start kafka consumer, error: {}", e));
                    let _ = start_signal.send(-1);
                    return;
                }
            };

        let _ = consumer.assign(&topic_partition);
        let _ = start_signal.send(0);

        loop {
            if cancellation_token.is_cancelled() {
                break;
            }

            match consumer.recv().await {
                Err(e) => {
                    println!("[rs-consumer] Kafka error");
                    // TODO: log unprocessed event and continue?
                }
                Ok(message) => {
                    if let Some(bytes) = rdkafka::Message::payload(&message) {
                        match IKVDataEvent::parse_from_bytes(bytes) {
                            Ok(event) => {
                                // TODO: handle errors
                                println!("[rs-consumer] Processing event.");
                                let _ = processor.process(&event);
                            }
                            Err(e) => {
                                // TODO: log deserialization errors
                                println!("[rs-consumer] Proto deserialization error");
                            }
                        }
                    }

                    // TODO: handle error while committing
                    let _ = consumer.commit_message(&message, CommitMode::Sync);
                }
            };
        }
    }

    pub fn stop(self) {
        self.cancellation_token.cancel();
        self.tokio_runtime.shutdown_timeout(Duration::from_secs(60));
    }
}

pub struct IKVKafkaConsumerContext;

impl ClientContext for IKVKafkaConsumerContext {}

impl ConsumerContext for IKVKafkaConsumerContext {
    fn commit_callback(&self, result: KafkaResult<()>, offsets: &TopicPartitionList) {
        // Store offsets on disk.
        // TODO! todo!()
    }
}
