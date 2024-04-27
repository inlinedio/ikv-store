use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use log::info;

use crate::index::ckv::CKVIndex;
use crate::kafka::consumer::IKVKafkaConsumer;
use crate::kafka::processor::WritesProcessor;
use crate::proto::generated_proto::common::IKVStoreConfig;
use crate::proto::generated_proto::index::CKVIndexHeader;

use super::index_loader;

pub struct IndexBuilder {}

impl IndexBuilder {
    // NOTE: callers must cleanup their working directories

    pub fn build_and_export(config: &IKVStoreConfig) -> anyhow::Result<()> {
        // Download and load previous base index
        info!("Loading previous base index");
        index_loader::load_index(config)?;
        let index = CKVIndex::open_or_create(config)?;

        let arc_index = Arc::new(index);

        // process writes till high watermark
        {
            info!("Consuming pending write events till high watermark.");
            let processor = Arc::new(WritesProcessor::new(arc_index.clone()));
            let kafka_consumer = IKVKafkaConsumer::new(config, processor.clone())?;
            kafka_consumer.blocking_run_till_completion()?;
            kafka_consumer.stop();
        }
        let index = Arc::try_unwrap(arc_index).expect("there should be no other references");

        info!("Starting index compaction.");
        // set headers - date time of data present in this index.
        let mut header = CKVIndexHeader::new();
        header.base_index_epoch_millis =
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
        index.write_index_header(&header)?;

        // in-place compaction
        index.compact_and_close()?;

        // upload to S3
        info!("Uploading base index to S3.");
        index_loader::upload_index(config)?;

        info!("Base index build and upload successful.");
        Ok(())
    }
}
