use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail};
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

use super::processor::WritesProcessor;

#[cfg(test)]
#[path = "consumer_test.rs"]
mod consumer_test;

pub struct IKVKafkaConsumer {
    tokio_runtime: Runtime,
    writes_processor: Arc<WritesProcessor>,

    // consumer thread
    async_consumer_channel: (Sender<anyhow::Result<()>>, Receiver<anyhow::Result<()>>),
    cancellation_token: CancellationToken,

    // Consumer configuration - created in constructor
    client_config: ClientConfig,
    topic_partition: TopicPartitionList,
}

impl IKVKafkaConsumer {
    /// Create a new consumer.
    pub fn new(config: &IKVStoreConfig, processor: Arc<WritesProcessor>) -> anyhow::Result<Self> {
        let kafka_consumer_bootstrap_server = config
            .stringConfigs
            .get("kafka_consumer_bootstrap_server")
            .ok_or(rdkafka::error::KafkaError::ClientCreation(
                "kafka_consumer_bootstrap_server is a required config".to_string(),
            ))?;

        // TODO: we might need SSL access
        // Ref: https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html
        let client_config = ClientConfig::new()
            .set("group.id", "ikv-default-consumer") // we don't use offset management or automatic partition assignment
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
        let topic = config.stringConfigs.get("kafka_topic").ok_or(
            rdkafka::error::KafkaError::ClientCreation(
                "kafka_topic is a required config".to_string(),
            ),
        )?;
        let partition = config.numericConfigs.get("kafka_partition").ok_or(
            rdkafka::error::KafkaError::ClientCreation(
                "kafka_partition is a required config".to_string(),
            ),
        )?;
        let partition = if (*partition > i32::MAX as i64) || (*partition < 0) {
            bail!("kafka_partition bad value: {}", partition);
        } else {
            *partition as i32
        };

        // TODO: read offset from offset store
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
            writes_processor: processor,
            async_consumer_channel: mpsc::channel(),
            cancellation_token: CancellationToken::new(),
            client_config: client_config,
            topic_partition: topic_partition,
        })
    }

    pub fn run_in_background(&self) -> anyhow::Result<()> {
        // start consumer thread
        self.tokio_runtime.spawn(IKVKafkaConsumer::run(
            self.writes_processor.clone(),
            self.async_consumer_channel.0.clone(),
            self.cancellation_token.clone(),
            self.client_config.clone(),
            self.topic_partition.clone(),
        ));

        // wait for startup of kafka thread
        match self.async_consumer_channel.1.recv() {
            Ok(signal) => {
                if let Err(e) = signal {
                    bail!(
                        "Cannot initialize write ingestion, error: {}",
                        e.to_string()
                    );
                } else {
                    Ok(())
                }
            }
            Err(e) => {
                // Async thread got killed
                bail!(
                    "Async writes processing thread killed, error: {}",
                    e.to_string()
                );
            }
        }
    }

    // blocking run.
    async fn run(
        writes_processor: Arc<WritesProcessor>,
        async_consumer_channel: Sender<anyhow::Result<()>>,
        cancellation_token: CancellationToken,
        client_config: ClientConfig,
        topic_partition: TopicPartitionList,
    ) {
        let consumer: StreamConsumer<IKVKafkaConsumerContext> =
            match client_config.create_with_context(IKVKafkaConsumerContext) {
                Ok(consumer) => consumer,
                Err(e) => {
                    let _ = async_consumer_channel.send(Err(anyhow!(
                        "Cannot create kafka StreamConsumer, error: {}",
                        e.to_string()
                    )));
                    return;
                }
            };

        if let Err(e) = consumer.assign(&topic_partition) {
            let _ = async_consumer_channel.send(Err(anyhow!(
                "Cannot assign kafka consumer to topic-partition, error: {}",
                e.to_string()
            )));
            return;
        }

        // TODO: lag consumption - inspect headers, inspect time and consume till under SLA.

        // Successful startup!
        let _ = async_consumer_channel.send(Ok(()));

        loop {
            if cancellation_token.is_cancelled() {
                break;
            }

            match consumer.recv().await {
                Err(e) => {
                    // Consumption initialization errors will be thrown here - ex. incorrect
                    // topic or partition, broker not being available, etc.

                    // We should inspect type of error code and see if
                    // we need to kill startup

                    // TODO: add logging for unprocessed event
                    eprintln!(
                        "Receiving kafka error in background loop: {}",
                        e.to_string()
                    )
                }
                Ok(message) => {
                    if let Some(bytes) = rdkafka::Message::payload(&message) {
                        match IKVDataEvent::parse_from_bytes(bytes) {
                            Ok(event) => {
                                writes_processor.process(&event);
                            }
                            Err(_e) => {
                                // TODO: add logging for unprocessed event
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
