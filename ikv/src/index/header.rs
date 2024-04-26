use std::{
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, Write},
    path::Path,
    sync::RwLock,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::bail;
use protobuf::Message;

use crate::proto::generated_proto::index::CKVIndexHeader;

/// Manages index level headers
#[derive(Debug)]
pub struct HeaderStore {
    lock: RwLock<()>,
    file: File,
}

// TODO: implementation is very similar to offset_store.rs
// Reuser.
impl HeaderStore {
    fn new(mount_directory: &str) -> anyhow::Result<Self> {
        let filename = format!("{}/header", mount_directory);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(filename)?;

        let store = HeaderStore {
            lock: RwLock::new(()),
            file,
        };

        let mut header = CKVIndexHeader::new();
        header.base_index_epoch_millis =
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
        store.write_header(&header)?;

        Ok(store)
    }

    pub fn open_or_create(mount_directory: &str) -> anyhow::Result<Self> {
        let filename = format!("{}/header", mount_directory);
        if !Path::new(&filename).exists() {
            // does not exist on disk, create new
            return HeaderStore::new(mount_directory);
        }

        // Exists on disk
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(filename)?;

        Ok(Self {
            lock: RwLock::new(()),
            file,
        })
    }

    pub fn index_not_present(mount_directory: &str) -> bool {
        let filepath = format!("{}/header", mount_directory);
        !Path::new(&filepath).exists()
    }

    pub fn delete_all(mount_directory: &str) -> anyhow::Result<()> {
        let filepath = format!("{}/header", mount_directory);
        if Path::new(&filepath).exists() {
            std::fs::remove_file(&filepath)?;
        }
        Ok(())
    }

    // checks if a valid index is loaded at the mount directory
    // Returns error with some details if empty or invalid, else ok.
    pub fn is_valid_index(mount_directory: &str) -> anyhow::Result<()> {
        let filepath = format!("{}/header", mount_directory);
        if !Path::new(&filepath).exists() {
            bail!("index header not present");
        }
        Ok(())
    }

    pub fn read_header(&self) -> anyhow::Result<CKVIndexHeader> {
        let mut bytes = Vec::new();
        {
            let _guard = self.lock.read().unwrap();
            let mut reader = BufReader::new(&self.file);
            reader.rewind()?;
            reader.read_to_end(&mut bytes)?;
        }

        if bytes.len() == 0 {
            bail!("ckv index header not present on disk.");
        }

        let header = CKVIndexHeader::parse_from_bytes(&bytes)?;
        Ok(header)
    }

    pub fn write_header(&self, header: &CKVIndexHeader) -> anyhow::Result<()> {
        let bytes = header.write_to_bytes()?;

        let _guard = self.lock.write();

        let mut writer = BufWriter::new(&self.file);
        writer.rewind()?;
        writer.write_all(&bytes)?;
        writer.flush()?;

        Ok(())
    }
}
