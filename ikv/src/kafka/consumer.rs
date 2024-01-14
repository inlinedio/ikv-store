use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail};
use log::{error, info, warn};
use rdkafka::consumer::DefaultConsumerContext;
use rdkafka::message::Message;
use rdkafka::util::Timeout;
use rdkafka::Offset;
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    ClientConfig, TopicPartitionList,
};
use tokio::runtime::{Builder, Runtime};
use tokio_util::sync::CancellationToken;

use crate::index::offset_store::OffsetStore;
use crate::proto::generated_proto::{common::IKVStoreConfig, streaming::IKVDataEvent};

use super::offset_committer::OffsetCommitter;
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
    pub fn new(config: &IKVStoreConfig, processor: Arc<WritesProcessor>) -> anyhow::Result<Self> {
        let mount_directory = crate::utils::paths::create_mount_directory(&config)?;

        let account_id = config.stringConfigs.get("account_id").ok_or(
            rdkafka::error::KafkaError::ClientCreation(
                "account_id is a required client-specified config".to_string(),
            ),
        )?;

        let account_passkey = config.stringConfigs.get("account_passkey").ok_or(
            rdkafka::error::KafkaError::ClientCreation(
                "account_passkey is a required client-specified config".to_string(),
            ),
        )?;

        let kafka_consumer_bootstrap_server = config
            .stringConfigs
            .get("kafka_bootstrap_server")
            .ok_or(rdkafka::error::KafkaError::ClientCreation(
                "kafka_bootstrap_server is a required gateway-specified config".to_string(),
            ))?;

        // Ref: https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html
        let client_config = ClientConfig::new()
            .set("group.id", "ikv-default-consumer") // we don't use offset management or automatic partition assignment
            .set("bootstrap.servers", kafka_consumer_bootstrap_server)
            .set("enable.partition.eof", "true")
            .set("session.timeout.ms", "3600000")
            .set("max.poll.interval.ms", "3600000")
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("security.protocol", "SASL_SSL")
            .set("sasl.mechanisms", "SCRAM-SHA-512")
            .set("sasl.username", account_id)
            .set("sasl.password", account_passkey)
            .clone();

        // topic and parition
        let topic = config.stringConfigs.get("kafka_topic").ok_or(
            rdkafka::error::KafkaError::ClientCreation(
                "kafka_topic is a required gateway-specified config".to_string(),
            ),
        )?;
        let partition = config.intConfigs.get("partition").ok_or(
            rdkafka::error::KafkaError::ClientCreation(
                "partition is a required user-specified config".to_string(),
            ),
        )?;
        let partition = if (*partition > i32::MAX as i64) || (*partition < 0) {
            bail!("partition bad value: {}", partition);
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
            client_config,
            topic: topic.to_string(),
            partition,
        })
    }

    /// Consumes all pending events, and consume all new incoming events.
    /// Can be stopped by invoking stop()
    /// TODO: if the consumer thread panics, there is currently no early return - dangerous!
    pub fn run_in_background(&self) -> anyhow::Result<()> {
        let offset_store = Arc::new(OffsetStore::open_or_create(self.mount_directory.clone())?);
        let offset_committer = Arc::new(OffsetCommitter::new(offset_store.clone()));

        // start consumer thread
        self.tokio_runtime.spawn(IKVKafkaConsumer::run(
            offset_store.clone(),
            self.writes_processor.clone(),
            offset_committer.clone(),
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

    /// Stop run_in_background() message consumption.
    pub fn stop(self) {
        self.cancellation_token.cancel();
        self.tokio_runtime.shutdown_timeout(Duration::from_secs(60));
    }

    /// Consumes all pending events
    pub fn blocking_run_till_completion(&self) -> anyhow::Result<()> {
        let offset_store = Arc::new(OffsetStore::open_or_create(self.mount_directory.clone())?);
        let offset_committer = Arc::new(OffsetCommitter::new(offset_store.clone()));

        // start consumer thread
        let handle = self.tokio_runtime.spawn(IKVKafkaConsumer::run(
            offset_store.clone(),
            self.writes_processor.clone(),
            offset_committer.clone(),
            self.async_consumer_channel.0.clone(),
            self.cancellation_token.clone(),
            true,
            self.client_config.clone(),
            self.topic.clone(),
            self.partition,
        ));

        // wait for finish signal
        match self.async_consumer_channel.1.recv() {
            Ok(signal) => {
                if let Err(e) = signal {
                    bail!("Cannot finish write processing, error: {}", e.to_string());
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

        // cleanup tokio thread
        self.tokio_runtime.block_on(handle)?;

        Ok(())
    }

    // blocking run.
    async fn run(
        offset_store: Arc<OffsetStore>,
        writes_processor: Arc<WritesProcessor>,
        offset_committer: Arc<OffsetCommitter>,
        async_consumer_channel: Sender<anyhow::Result<()>>,
        cancellation_token: CancellationToken,
        stop_at_high_watermark: bool,
        client_config: ClientConfig,
        topic: String,
        partition: i32,
    ) {
        info!("Initializing kafka stream consumer.");
        let consumer = match initialize(offset_store, &client_config, &topic, partition).await {
            Ok(c) => c,
            Err(e) => {
                let _ = async_consumer_channel.send(Err(anyhow!(
                    "Cannot initialize kafka StreamConsumer, error: {}",
                    e.to_string()
                )));
                return;
            }
        };

        // Consume lag or existing events for offline index build
        info!("Starting consumption of pending writes.");
        if let Err(e) = consume_till_high_watermark(
            &consumer,
            writes_processor.clone(),
            offset_committer.clone(),
            &topic,
            partition,
        )
        .await
        {
            let _ = async_consumer_channel.send(Err(anyhow!(
                "Cannot consume pending writes. Error: {}",
                e.to_string()
            )));
            return;
        }

        // Successful startup!
        info!("All pending writes consumed, kafka startup successful.");
        let _ = async_consumer_channel.send(Ok(()));
        if stop_at_high_watermark {
            return;
        }

        if let Err(e) = consume_till_cancelled(
            &consumer,
            writes_processor.clone(),
            offset_committer.clone(),
            cancellation_token,
        )
        .await
        {
            error!("Write processor thread has crashed. Try to resolve and restart application. Error: {}", e.to_string())
        }
    }
}

async fn initialize(
    offset_store: Arc<OffsetStore>,
    client_config: &ClientConfig,
    topic: &str,
    partition: i32,
) -> anyhow::Result<StreamConsumer<DefaultConsumerContext>> {
    let consumer = client_config.create_with_context(DefaultConsumerContext)?;

    // initialize - by starting at the very beginning of the topic.
    seek_consumer(&consumer, topic, partition, rdkafka::Offset::Beginning)?;

    // seek - using persisted offsets
    let stored_topic_partition_list = offset_store.read_all_offsets()?;
    for entry in stored_topic_partition_list.iter() {
        if (&entry.topic == topic) && (entry.partition == partition) {
            let raw_offset = entry.offset;
            let offset = Offset::from_raw(raw_offset);
            seek_consumer(&consumer, topic, partition, offset)?;
            break;
        }
    }

    Ok(consumer)
}

fn seek_consumer(
    consumer: &StreamConsumer<DefaultConsumerContext>,
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

async fn consume_till_high_watermark(
    consumer: &StreamConsumer<DefaultConsumerContext>,
    writes_processor: Arc<WritesProcessor>,
    offset_committer: Arc<OffsetCommitter>,
    topic: &str,
    partition: i32,
) -> anyhow::Result<()> {
    // current point in time watermarks
    let current_low_watermark: i64;
    let current_high_watermark: i64;
    match consumer.fetch_watermarks(
        &topic,
        partition,
        Timeout::After(Duration::from_secs(60 * 5)),
    ) {
        Ok((low, high)) => {
            current_low_watermark = low;
            current_high_watermark = high;
        }
        Err(e) => {
            bail!("Cannot fetch watermarks, error: {}", e.to_string());
        }
    }

    if current_low_watermark == current_high_watermark {
        // empty topic
        return Ok(());
    }

    // Lag consumption, i.e. consume till offset: current_end_offset
    let end_offset = Offset::from_raw(current_high_watermark);
    if end_offset == Offset::Invalid || end_offset.to_raw().is_none() {
        bail!(
            "Invalid offset created from current_high_watermark: {}",
            current_high_watermark
        );
    }

    loop {
        // recv() is cancellation safe - ie exits
        // when tokio runtime is shutdown or task is abort()'ed
        match consumer.recv().await {
            Err(e) => match e {
                rdkafka::error::KafkaError::PartitionEOF(_) => return Ok(()),
                e => return Err(e.into()),
            },
            Ok(curr_message) => {
                if let Some(bytes) = rdkafka::Message::payload(&curr_message) {
                    let event = <IKVDataEvent as protobuf::Message>::parse_from_bytes(bytes)?;
                    writes_processor.process(&event)?;

                    let end_offset = end_offset.to_raw().expect("end_offset is pre-validated");
                    if curr_message.offset() >= end_offset {
                        writes_processor.flush_all()?;
                        offset_committer.commit(
                            curr_message.topic(),
                            curr_message.partition(),
                            curr_message.offset(),
                        )?;
                        return Ok(());
                    }
                }
            }
        };
    }
}

async fn consume_till_cancelled(
    consumer: &StreamConsumer<DefaultConsumerContext>,
    writes_processor: Arc<WritesProcessor>,
    offset_committer: Arc<OffsetCommitter>,
    cancellation_token: CancellationToken,
) -> anyhow::Result<()> {
    loop {
        if cancellation_token.is_cancelled() {
            return Ok(());
        }

        // recv() is cancellation safe - ie exits
        // when tokio runtime is shutdown or task is abort()'ed
        match consumer.recv().await {
            Err(e) => {
                warn!(
                    "Encountered kafka error (non fatal) - sleep then retry. Error: {}",
                    e.to_string()
                );

                // 100ms sleep and try again
                std::thread::sleep(Duration::from_millis(100));
            }
            Ok(curr_message) => {
                if let Some(bytes) = rdkafka::Message::payload(&curr_message) {
                    let event = <IKVDataEvent as protobuf::Message>::parse_from_bytes(bytes)?;
                    writes_processor.process(&event)?;

                    // flush index and commit offset in batches
                    if offset_committer.should_commit() {
                        writes_processor.flush_all()?;
                        offset_committer.commit(
                            curr_message.topic(),
                            curr_message.partition(),
                            curr_message.offset(),
                        )?;
                    }
                }
            }
        };
    }
}
