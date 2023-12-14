use std::{
    fs::{File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, Write},
    path::Path,
    sync::RwLock,
};

use anyhow::anyhow;
use protobuf::Message;
use rdkafka::TopicPartitionList;

use crate::proto::generated_proto::index::{KafkaOffsetStore, KafkaOffsetStoreEntry};

pub struct OffsetStore {
    lock: RwLock<()>,
    file: File,
}

impl OffsetStore {
    pub fn open_or_create(mount_directory: String) -> io::Result<Self> {
        let filename = format!("{}/index/kafka_offsets", mount_directory);
        let file;

        if !Path::new(&filename).exists() {
            // does not exist on disk
            file = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(filename)?;
        } else {
            // Exists on disk
            file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(false)
                .open(filename)?;
        }

        Ok(Self {
            lock: RwLock::new(()),
            file,
        })
    }

    pub fn read_all_offsets(&self) -> anyhow::Result<Vec<KafkaOffsetStoreEntry>> {
        let _guard = self.lock.read().unwrap();

        let mut bytes = Vec::new();

        let mut reader = BufReader::new(&self.file);
        reader.rewind()?;
        reader.read_to_end(&mut bytes)?;

        let kafka_offset_store = KafkaOffsetStore::parse_from_bytes(&bytes)?;

        let mut entries = Vec::new();
        for entry in kafka_offset_store.entries {
            entries.push(entry);
        }

        Ok(entries)
    }

    pub fn write_all_offsets(
        &self,
        topic_partition_list: &TopicPartitionList,
    ) -> anyhow::Result<()> {
        let _guard = self.lock.write().unwrap();

        let mut entries = Vec::new();
        for elt in topic_partition_list.elements() {
            let mut entry = KafkaOffsetStoreEntry::new();
            entry.topic = elt.topic().to_string();
            entry.partition = elt.partition();
            entry.offset = elt.offset().to_raw().ok_or(anyhow!(""))?;
            entries.push(entry);
        }

        let mut kafka_offset_store = KafkaOffsetStore::new();
        kafka_offset_store.entries = entries;

        let bytes = kafka_offset_store.write_to_bytes()?;

        let mut writer = BufWriter::new(&self.file);
        writer.rewind()?;
        writer.write_all(&bytes)?;
        writer.flush()?;

        Ok(())
    }
}