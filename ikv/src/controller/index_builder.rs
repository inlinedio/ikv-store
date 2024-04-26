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
    index: CKVIndex,
}

impl IndexBuilder {
    pub fn new(config: &IKVStoreConfig) -> anyhow::Result<Self> {
        // Load index
        index_loader::load_index(config)?;
        let index = CKVIndex::open_or_create(config)?;

        Ok(Self { index })
    }

    // NOTE: callers must cleanup their working directories
    pub fn build_and_export(self, config: &IKVStoreConfig) -> anyhow::Result<()> {
        info!("Starting base index build.");

        // set index headers
        {
            let mut header = CKVIndexHeader::new();
            header.base_index_epoch_millis =
                SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
            self.index.write_index_header(&header)?;
        }

        let arc_index = Arc::new(self.index);

        // process writes till high watermark
        {
            let processor = Arc::new(WritesProcessor::new(arc_index.clone()));
            let kafka_consumer = IKVKafkaConsumer::new(config, processor.clone())?;
            kafka_consumer.blocking_run_till_completion()?;
            kafka_consumer.stop();
        }

        let mut index = Arc::try_unwrap(arc_index).expect("there should be no other references");

        // index compaction
        info!("Starting base index compaction.");
        index.compact()?;
        index.close()?;
        CKVIndex::swap_compacted_segments(config)?;

        // upload to S3
        info!("Uploading base index to S3.");
        index_loader::upload_index(config)?;

        info!("Base index build and upload successful.");
        Ok(())
    }
}
