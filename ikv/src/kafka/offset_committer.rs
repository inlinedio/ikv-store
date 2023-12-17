use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

use rdkafka::{Offset, TopicPartitionList};

use crate::index::offset_store::OffsetStore;

// TODO: make this a user-specified config
const BATCH_SIZE: u32 = 100;

pub struct OffsetCommitter {
    counter: AtomicU32,
    offset_store: Arc<OffsetStore>,
}

impl OffsetCommitter {
    pub fn new(offset_store: Arc<OffsetStore>) -> Self {
        Self {
            // start from 1 to avoid committing for the very first should_commit call
            counter: AtomicU32::new(1),
            offset_store,
        }
    }

    pub fn should_commit(&self) -> bool {
        // This operation wraps around on overflow.
        let prev = self.counter.fetch_add(1, Ordering::SeqCst);
        prev % BATCH_SIZE == 0
    }

    pub fn commit(&self, topic: &str, partition: i32, offset: i64) -> anyhow::Result<()> {
        let mut topic_partition_list = TopicPartitionList::new();
        topic_partition_list.add_partition_offset(topic, partition, Offset::from_raw(offset))?;
        self.offset_store.write_all_offsets(&topic_partition_list)?;
        Ok(())
    }
}
