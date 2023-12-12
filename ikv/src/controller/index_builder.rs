use std::sync::Arc;

use anyhow::anyhow;

use crate::index::ckv::CKVIndex;
use crate::kafka::consumer::IKVKafkaConsumer;
use crate::kafka::processor::WritesProcessor;
use crate::proto::generated_proto::common::IKVStoreConfig;

pub struct IndexBuilder {
    index: Arc<CKVIndex>,
    kafka_consumer: IKVKafkaConsumer,
}

impl IndexBuilder {
    pub fn new(config: IKVStoreConfig) -> anyhow::Result<Self> {
        let mount_directory = config
            .stringConfigs
            .get("mount_directory")
            .ok_or(anyhow!("mount_directory is a required config"))?;

        // 2. Open index
        let index = CKVIndex::open_or_create(mount_directory.clone(), &config)?;
        let index = Arc::new(index);

        // 3. Initialize kafka consumer
        let processor = Arc::new(WritesProcessor::new(index.clone()));
        let kafka_consumer =
            IKVKafkaConsumer::new(mount_directory.clone(), &config, processor.clone())?;

        todo!()
    }

    pub fn build_and_export(&self) -> anyhow::Result<()> {
        // start write processing
        // blocks to consume pending messages
        self.kafka_consumer.blocking_run_till_completion()?;
        self.index.export()?;
        Ok(())
    }
}
