use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use log::info;

use crate::index::ckv::CKVIndex;
use crate::kafka::consumer::IKVKafkaConsumer;
use crate::kafka::processor::WritesProcessor;
use crate::proto::generated_proto::common::IKVStoreConfig;
use crate::proto::generated_proto::index::CKVIndexHeader;

use super::index_loader;

pub struct IndexBuilder {
    index: Arc<CKVIndex>,
    kafka_consumer: IKVKafkaConsumer,
}

impl IndexBuilder {
    pub fn new(config: &IKVStoreConfig) -> anyhow::Result<Self> {
        // Load index
        index_loader::load_index(config)?;
        let index = Arc::new(CKVIndex::open_or_create(config)?);

        // Initialize kafka consumer
        let processor = Arc::new(WritesProcessor::new(index.clone()));
        let kafka_consumer = IKVKafkaConsumer::new(config, processor.clone())?;

        Ok(Self {
            index,
            kafka_consumer,
        })
    }

    // NOTE: callers must cleanup their working directories
    pub fn build_and_export(&self, config: &IKVStoreConfig) -> anyhow::Result<()> {
        info!("Starting base index build.");

        // set index headers
        {
            let mut header = CKVIndexHeader::new();
            header.base_index_epoch_millis =
                SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
            self.index.write_index_header(&header)?;
        }

        // start write processing
        // blocks to consume pending messages
        self.kafka_consumer.blocking_run_till_completion()?;
        info!("Starting base index compaction.");

        // index compaction
        self.index.compact()?;

        // upload to S3
        info!("Uploading base index to S3.");
        index_loader::upload_index(config)?;

        info!("Base index build and upload successful.");
        Ok(())
    }

    pub fn close(self) -> anyhow::Result<()> {
        self.kafka_consumer.stop();
        info!("Closing IKV, Bye Bye.");
        Ok(())
    }
}
