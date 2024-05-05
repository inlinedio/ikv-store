use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use log::debug;
use protobuf::Message;
use rdkafka::error::KafkaError;
use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use tokio::runtime::{Builder, Runtime};

use anyhow::{anyhow, bail};

use crate::proto::generated_proto::common::FieldValue;
use crate::proto::generated_proto::{common::IKVStoreConfig, streaming::IKVDataEvent};

pub struct IKVKafkaProducer {
    tokio_runtime: Runtime,
    sender: Sender<(KafkaMessage, KafkaSendCondVar)>,
    partitions: i32,
}

impl IKVKafkaProducer {
    pub fn new(config: &IKVStoreConfig) -> anyhow::Result<Self> {
        // Producer client config
        let client_config = create_producer_cfg(config)?;

        // message creation params
        let topic = config
            .stringConfigs
            .get("kafka_topic")
            .ok_or(anyhow!(
                "kafka_topic is a required gateway-specified config".to_string(),
            ))
            .cloned()?;

        let partitions: i32 = config
            .intConfigs
            .get("num_kafka_partitions")
            .ok_or(anyhow!(
                "num_kafka_partitions is a required gateway-specified config".to_string(),
            ))
            .cloned()?
            .try_into()?;

        let (sender, receiver) = channel();

        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("kafka-producer-thread")
            .enable_time()
            .build()?;

        // start background thread which waits on new
        // events and writes them to kafka
        runtime.spawn(IKVKafkaProducer::wait_for_events_and_write(
            topic.clone(),
            client_config,
            receiver,
        ));

        Ok(Self {
            tokio_runtime: runtime,
            sender,
            partitions,
        })
    }

    pub fn close(self) -> anyhow::Result<()> {
        drop(self.sender);
        self.tokio_runtime.shutdown_timeout(Duration::from_secs(60));
        Ok(())
    }

    // for upsert, delete operations
    pub fn write_to_single_partition(
        &self,
        field_value: &FieldValue,
        event: &IKVDataEvent,
    ) -> anyhow::Result<()> {
        // create message
        let serialized_field_value = field_value.write_to_bytes()?;
        let serialized_ikv_data_event = event.write_to_bytes()?;
        let kafka_message = KafkaMessage {
            partition: None,
            serialized_field_value,
            serialized_ikv_data_event,
        };

        // write (send to channel, wait on callback)
        let callback = KafkaSendCondVar::new();
        self.sender.send((kafka_message, callback.clone()))?;
        callback.wait()
    }

    // for drop operations
    pub fn write_to_all_partitions(&self, event: &IKVDataEvent) -> anyhow::Result<()> {
        let serialized_field_value = FieldValue::new().write_to_bytes()?;
        let serialized_ikv_data_event = event.write_to_bytes()?;

        // write to all partitions
        for partition in 0..self.partitions {
            // create message
            let kafka_message = KafkaMessage {
                partition: Some(partition),
                serialized_field_value: serialized_field_value.clone(),
                serialized_ikv_data_event: serialized_ikv_data_event.clone(),
            };
            // send to channel, wait on callback
            let callback = KafkaSendCondVar::new();
            self.sender.send((kafka_message, callback.clone()))?;
            callback.wait()?;
        }

        Ok(())
    }

    // TODO: add an init event startup to test connection
    async fn wait_for_events_and_write(
        topic: String,
        client_config: ClientConfig,
        message_stream: Receiver<(KafkaMessage, KafkaSendCondVar)>,
    ) -> anyhow::Result<()> {
        // initialize producer
        let producer: FutureProducer = client_config.create()?;

        // loop and wait for events
        loop {
            let event = message_stream.recv();
            if event.is_err() {
                // all senders have disconnected, exit.
                debug!("Shutting down kafka writer thread");
                break;
            }

            let (message, callback) = event.unwrap();
            let mut future_record = FutureRecord::to(&topic)
                .key(&message.serialized_field_value)
                .payload(&message.serialized_ikv_data_event);
            if let Some(partition) = message.partition {
                future_record = future_record.partition(partition);
            }

            let send_result = producer.send(future_record, Duration::from_secs(0)).await;
            match send_result {
                Ok(_) => callback.notify_ok(),
                Err((e, _)) => callback.notify_err(e),
            }
        }

        Ok(())
    }
}

fn create_producer_cfg(config: &IKVStoreConfig) -> anyhow::Result<ClientConfig> {
    let account_id = config.stringConfigs.get("account_id").ok_or(anyhow!(
        "account_id is a required client-specified config".to_string(),
    ))?;

    let account_passkey = config.stringConfigs.get("account_passkey").ok_or(anyhow!(
        "account_passkey is a required client-specified config".to_string(),
    ))?;

    let bootstrap_servers = config
        .stringConfigs
        .get("kafka_bootstrap_server")
        .ok_or(anyhow!(
            "kafka_bootstrap_server is a required gateway-specified config".to_string(),
        ))?;

    let mut client_config = ClientConfig::new();
    client_config
        .set("bootstrap.servers", bootstrap_servers)
        .set("security.protocol", "SASL_SSL")
        .set("sasl.mechanisms", "SCRAM-SHA-512")
        .set("sasl.username", account_id)
        .set("sasl.password", account_passkey)
        .set("enable.ssl.certificate.verification", "false");

    Ok(client_config)
}

struct KafkaMessage {
    partition: Option<i32>,
    serialized_field_value: Vec<u8>,
    serialized_ikv_data_event: Vec<u8>,
}

#[derive(Clone)]
struct KafkaSendCondVar {
    condvar: Arc<(Mutex<anyhow::Result<()>>, Condvar)>,
}

impl KafkaSendCondVar {
    pub fn new() -> Self {
        Self {
            condvar: Arc::new((Mutex::new(Ok(())), Condvar::new())),
        }
    }

    pub fn notify_ok(&self) {
        let mut result = self.condvar.0.lock().unwrap();
        *result = Ok(());
        self.condvar.1.notify_all();
    }

    pub fn notify_err(&self, err: KafkaError) {
        let mut result = self.condvar.0.lock().unwrap();
        *result = Err(err.into());
        self.condvar.1.notify_all();
    }

    pub fn wait(&self) -> anyhow::Result<()> {
        let mutex_guard = self.condvar.0.lock().unwrap();
        let result = self.condvar.1.wait(mutex_guard).unwrap();
        if let Some(e) = result.as_ref().err() {
            bail!("Kafka send error: {}", e)
        }

        Ok(())
    }
}
