use std::sync::Arc;

use anyhow::anyhow;

use crate::index::ckv::CKVIndex;
use crate::kafka::consumer::IKVKafkaConsumer;
use crate::kafka::processor::WritesProcessor;
use crate::proto::generated_proto::common::IKVStoreConfig;

/// Stateful controller for managing IKV core key-val storage.
pub struct Controller {
    // ref to index
    index: Arc<CKVIndex>,

    // ref to kafka consumer
    processor: Arc<WritesProcessor>,
    kafka_consumer: IKVKafkaConsumer,
}

impl Controller {
    pub fn open(client_supplied_config: IKVStoreConfig) -> anyhow::Result<Self> {
        // TODO: Fetch cloud configs with GRPC call - ex. Kafka consumer configs
        // Assume client supplies all required configs
        let config = Controller::merged_config(client_supplied_config)?;

        let mount_directory = config
            .stringConfigs
            .get("mount_directory")
            .ok_or(anyhow!("mount_directory is a required config"))?;

        // 2. Open index - inspect if it exists locally, else fetch base index
        let index = CKVIndex::open_or_create(mount_directory.clone(), &config)?;
        let index = Arc::new(index);

        // 3. Start kafka consumption
        let processor = Arc::new(WritesProcessor::new(index.clone()));

        let kafka_consumer =
            IKVKafkaConsumer::new(mount_directory.clone(), &config, processor.clone())?;

        // start write processing
        // blocks to consume pending messages
        kafka_consumer.run_in_background()?;

        Ok(Controller {
            index,
            processor,
            kafka_consumer,
        })
    }

    fn merged_config(client_supplied_config: IKVStoreConfig) -> anyhow::Result<IKVStoreConfig> {
        // TODO: fetch configs from cloud!
        Ok(client_supplied_config)
    }

    /// Atomic reference to the index.
    pub fn index_ref(&self) -> Arc<CKVIndex> {
        self.index.clone()
    }

    /// Atomic reference to the writes processor.
    pub fn writes_processor_ref(&self) -> Arc<WritesProcessor> {
        self.processor.clone()
    }

    pub fn close(self) -> Result<(), String> {
        self.kafka_consumer.stop();
        self.index.close();
        Ok(())
    }
}
