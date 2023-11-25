use std::sync::Arc;

use crate::index::ckv::CKVIndex;
use crate::kafka::consumer::IKVKafkaConsumer;
use crate::kafka::processor::WritesProcessor;
use crate::proto::generated_proto::common::IKVStoreConfig;

/// Stateful controller for managing IKV core key-val storage.
pub struct Controller {
    // ref to index
    index: Arc<CKVIndex>,

    // ref to kafka consumer
    kafka_consumer: IKVKafkaConsumer,
    // ref (grpc client) to inline cloud
}

impl Controller {
    pub fn open(client_supplied_config: IKVStoreConfig) -> Result<Self, String> {
        // TODO: Fetch cloud configs with GRPC call - ex. Kafka consumer configs
        // Assume client supplies all required configs
        let config = Controller::merged_config(client_supplied_config)?;

        let mount_directory = config
            .stringConfigs
            .get("mount_directory")
            .ok_or("mount_directory is a required config".to_string())?;

        // 2. Open index - inspect if it exists locally, else fetch base index
        let index = match CKVIndex::open(mount_directory.clone(), &config) {
            Ok(index) => index,
            Err(e) => return Err(e.to_string()),
        };
        let index = Arc::new(index);

        // 3. Start kafka consumption
        let mut kafka_consumer =
            match IKVKafkaConsumer::new(&config, WritesProcessor::new(index.clone())) {
                Ok(kc) => kc,
                Err(e) => return Err(e.to_string()),
            };
        kafka_consumer.run_in_background();

        Ok(Controller {
            index,
            kafka_consumer,
        })
    }

    fn merged_config(client_supplied_config: IKVStoreConfig) -> Result<IKVStoreConfig, String> {
        // TODO: fetch configs from cloud!
        Ok(client_supplied_config)
    }

    /// Atomic reference to the index.
    pub fn index_ref(&self) -> Arc<CKVIndex> {
        self.index.clone()
    }

    pub fn close(self) -> Result<(), String> {
        self.kafka_consumer.stop();
        let _ = self.index.close();
        Ok(())
    }
}
