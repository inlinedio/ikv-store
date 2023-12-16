use std::sync::Arc;

use anyhow::anyhow;

use crate::index::ckv::CKVIndex;
use crate::kafka::consumer::IKVKafkaConsumer;
use crate::kafka::processor::WritesProcessor;
use crate::proto::generated_proto::common::IKVStoreConfig;

use super::index_loader;

pub struct IndexBuilder {
    index: Arc<CKVIndex>,
    kafka_consumer: IKVKafkaConsumer,
}

impl IndexBuilder {
    pub fn new(config: &IKVStoreConfig) -> anyhow::Result<Self> {
        let mount_directory = crate::utils::paths::create_mount_directory(config)?;

        // Load index
        index_loader::load_index(&config)?;
        let index = Arc::new(CKVIndex::open_or_create(config)?);

        // Initialize kafka consumer
        let processor = Arc::new(WritesProcessor::new(index.clone()));
        let kafka_consumer =
            IKVKafkaConsumer::new(mount_directory.clone(), config, processor.clone())?;

        Ok(Self {
            index,
            kafka_consumer,
        })
    }

    // NOTE: callers must cleanup their working directories
    pub fn build_and_export(&self, config: &IKVStoreConfig) -> anyhow::Result<()> {
        // start write processing
        // blocks to consume pending messages
        self.kafka_consumer.blocking_run_till_completion()?;
        self.index.compact()?;

        // TODO! upload index back to S3
        index_loader::upload_index(config)?;

        Ok(())
    }
}
