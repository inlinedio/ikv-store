use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail};
use rdkafka::message::Message;
use rdkafka::util::Timeout;
use rdkafka::Offset;
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{CommitMode, Consumer, StreamConsumer},
    ClientConfig, TopicPartitionList,
};
use tokio::runtime::{Builder, Runtime};
use tokio_util::sync::CancellationToken;

use crate::proto::generated_proto::{common::IKVStoreConfig, streaming::IKVDataEvent};

use super::context::IKVKafkaConsumerContext;
use super::offset_store::OffsetStore;
use super::processor::WritesProcessor;

#[cfg(test)]
#[path = "consumer_test.rs"]
mod consumer_test;

pub struct IKVKafkaConsumer {
    mount_directory: String,

    tokio_runtime: Runtime,
    writes_processor: Arc<WritesProcessor>,

    // consumer thread
    async_consumer_channel: (Sender<anyhow::Result<()>>, Receiver<anyhow::Result<()>>),
    cancellation_token: CancellationToken,

    // Consumer configuration - created in constructor
    client_config: ClientConfig,

    topic: String,
    partition: i32,
}

impl IKVKafkaConsumer {
    /// Create a new consumer.
    pub fn new(
        mount_directory: String,
        config: &IKVStoreConfig,
        processor: Arc<WritesProcessor>,
    ) -> anyhow::Result<Self> {
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

        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("kafka-consumer-thread")
            .enable_time()
            .build()?;

        Ok(IKVKafkaConsumer {
            mount_directory,
            tokio_runtime: runtime,
            writes_processor: processor,
            async_consumer_channel: mpsc::channel(),
            cancellation_token: CancellationToken::new(),
            client_config: client_config,
            topic: topic.to_string(),
            partition,
        })
    }

    // end_timestamp: Optional milliseconds since epoch
    pub fn run_in_background(&self) -> anyhow::Result<()> {
        let offset_store = OffsetStore::open_or_create(self.mount_directory.clone())?;

        // start consumer thread
        self.tokio_runtime.spawn(IKVKafkaConsumer::run(
            offset_store,
            self.writes_processor.clone(),
            self.async_consumer_channel.0.clone(),
            self.cancellation_token.clone(),
            false,
            self.client_config.clone(),
            self.topic.clone(),
            self.partition,
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

    /// Call when consuming till some end timestamp
    pub fn blocking_run_till_completion(&self) -> anyhow::Result<()> {
        let offset_store = OffsetStore::open_or_create(self.mount_directory.clone())?;

        // start consumer thread
        self.tokio_runtime.spawn(IKVKafkaConsumer::run(
            offset_store,
            self.writes_processor.clone(),
            self.async_consumer_channel.0.clone(),
            self.cancellation_token.clone(),
            true,
            self.client_config.clone(),
            self.topic.clone(),
            self.partition,
        ));

        // wait for startup of kafka thread
        match self.async_consumer_channel.1.recv() {
            Ok(signal) => {
                if let Err(e) = signal {
                    bail!(
                        "Cannot initialize write ingestion, error: {}",
                        e.to_string()
                    );
                }
            }
            Err(e) => {
                // Async thread got killed
                bail!(
                    "Async writes processing thread killed, error: {}",
                    e.to_string()
                );
            }
        };

        // wait for finish signal
        match self.async_consumer_channel.1.recv() {
            Ok(signal) => {
                if let Err(e) = signal {
                    bail!("Cannot finish write processing, error: {}", e.to_string());
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

    pub fn stop(self) {
        self.cancellation_token.cancel();
        self.tokio_runtime.shutdown_timeout(Duration::from_secs(60));
    }

    // blocking run.
    async fn run(
        offset_store: OffsetStore,
        writes_processor: Arc<WritesProcessor>,
        async_consumer_channel: Sender<anyhow::Result<()>>,
        cancellation_token: CancellationToken,
        stop_at_high_watermark: bool,
        client_config: ClientConfig,
        topic: String,
        partition: i32,
    ) {
        let consumer: StreamConsumer<IKVKafkaConsumerContext>;
        match initialize(offset_store, client_config, &topic, partition) {
            Ok(c) => consumer = c,
            Err(e) => {
                let _ = async_consumer_channel.send(Err(anyhow!(
                    "Cannot initialize kafka StreamConsumer, error: {}",
                    e.to_string()
                )));
                return;
            }
        }

        // TODO: move this inside initialize!
        let mut end_offset = None;
        if stop_at_high_watermark {
            match consumer.fetch_watermarks(&topic, partition, Timeout::Never) {
                Ok((_start, end)) => {
                    end_offset = Some(end);
                }
                Err(e) => {
                    let _ = async_consumer_channel.send(Err(anyhow!(
                        "Cannot fetch high watermark, error: {}",
                        e.to_string()
                    )));
                    return;
                }
            }
        }

        // TODO: lag consumption - inspect headers, inspect time and consume till under SLA.

        // Successful startup!
        let _ = async_consumer_channel.send(Ok(()));

        if cancellation_token.is_cancelled() {
            return;
        }

        loop {
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
                Ok(bmessage) => {
                    if let Some(bytes) = rdkafka::Message::payload(&bmessage) {
                        match <IKVDataEvent as protobuf::Message>::parse_from_bytes(bytes) {
                            Ok(event) => {
                                writes_processor.process(&event);
                            }
                            Err(_e) => {
                                // TODO: add logging for unprocessed event
                            }
                        }
                    }

                    // TODO: handle errors while flushing
                    let _ = writes_processor.flush_all();

                    // TODO: check if we need to commit - ex. batched commits.

                    // TODO: handle error while committing
                    let _ = consumer.commit_message(&bmessage, CommitMode::Sync);

                    if let Some(end_offset) = end_offset {
                        if bmessage.offset() >= end_offset {
                            break;
                        }
                    }
                }
            };

            if cancellation_token.is_cancelled() {
                break;
            }
        }

        // Finish signal
        let _ = async_consumer_channel.send(Ok(()));
    }
}

fn initialize(
    offset_store: OffsetStore,
    client_config: ClientConfig,
    topic: &str,
    partition: i32,
) -> anyhow::Result<StreamConsumer<IKVKafkaConsumerContext>> {
    let stored_topic_partition_list = offset_store.read_all_offsets()?;
    let consumer_context = IKVKafkaConsumerContext::new(offset_store);
    let consumer: StreamConsumer<IKVKafkaConsumerContext> =
        match client_config.create_with_context(consumer_context) {
            Ok(consumer) => consumer,
            Err(e) => {
                bail!(
                    "Cannot create kafka StreamConsumer, error: {}",
                    e.to_string()
                )
            }
        };

    // initialize - by starting at the very beginning of the topic.
    seek_consumer(&consumer, topic, partition, rdkafka::Offset::Beginning)?;

    // seek - using persisted offsets
    for entry in stored_topic_partition_list.iter() {
        if &entry.topic == topic && entry.partition == partition {
            let raw_offset = entry.offset;
            let offset = Offset::from_raw(raw_offset);
            seek_consumer(&consumer, topic, partition, offset)?;
        }
    }

    Ok(consumer)
}

fn seek_consumer(
    consumer: &StreamConsumer<IKVKafkaConsumerContext>,
    topic: &str,
    partition: i32,
    offset: Offset,
) -> anyhow::Result<()> {
    let mut topic_partition = TopicPartitionList::new();
    topic_partition.add_partition_offset(topic, partition, offset)?;
    if let Err(e) = consumer.assign(&topic_partition) {
        bail!(
            "Cannot assign kafka consumer to topic-partition, error: {}",
            e.to_string()
        );
    }
    Ok(())
}
