use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, Error, Write},
};

use memmap2::MmapMut;

use super::field::Field;

pub type Key = Vec<u8>;

pub struct PrimaryKeyIndex {
    index: HashMap<Key, Vec<usize>>,

    index_file: File,

    memory_map: MmapMut,
}

impl PrimaryKeyIndex {
    /// Creates a brand new empty instance of a primary-key index.
    pub fn new(index_id: u32) -> io::Result<PrimaryKeyIndex> {
        let index = HashMap::new();

        // hashmap persistence file
        let filename = format!("index_{}", index_id);
        let index_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(filename)?;

        // memmap file
        let filename = format!("mmap_{}", index_id);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(filename)?;
        let memory_map = unsafe { MmapMut::map_mut(&file)? };
        Ok(PrimaryKeyIndex {
            index,
            index_file,
            memory_map,
        })
    }

    /// Re-open a previously created index.
    pub fn open(index_id: u32) -> io::Result<PrimaryKeyIndex> {
        let index = HashMap::new();

        // hashmap persistence file
        let filename = format!("index_{}", index_id);
        let index_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(filename)?;

        // memmap file
        let filename = format!("mmap_{}", index_id);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(filename)?;
        let memory_map = unsafe { MmapMut::map_mut(&file)? };
        Ok(PrimaryKeyIndex {
            index,
            index_file,
            memory_map,
        })
    }

    /// Read bytes for a given key and field.
    pub fn read(&self, key: &Key, field: &Field) -> Option<Vec<u8>> {
        let offsets = self.index.get(key)?;
        let offset = offsets.get(field.id() as usize).copied()?;
        if offset == usize::MAX {
            return None;
        }

        let value = match field.value_len() {
            Some(value_len) => {
                // static size
                &self.memory_map[offset..offset + value_len]
            }
            None => {
                // dynamic size
                let value_len = &self.memory_map[offset..offset + 4];
                let value_len = u32::from_le_bytes(
                    value_len
                        .try_into()
                        .expect("persisted value_len must be 4 bytes in length"),
                ) as usize;
                &self.memory_map[offset + 4..offset + 4 + value_len]
            }
        };

        // deep copy result into vector
        let mut result = vec![0 as u8; value.len()];
        result[..].copy_from_slice(&value);
        Some(result)
    }

    /// Upsert value (bytes) for a given key.
    pub fn upsert(&mut self, key: Key, field: &Field, value: &[u8]) -> io::Result<()> {
        if key.len() == 0 {
            return Err(Error::new(
                std::io::ErrorKind::InvalidInput,
                "empty key not allowed",
            ));
        }

        if value.len() == 0 {
            return Err(Error::new(
                std::io::ErrorKind::InvalidInput,
                "empty value not allowed",
            ));
        }

        if value.len() > u32::MAX as usize {
            return Err(Error::new(
                std::io::ErrorKind::Unsupported,
                "values larger than 4GB are unsupported",
            ));
        }

        let field_id = field.id() as usize;

        let offsets = self.index.entry(key.clone()).or_default();
        if field_id as usize >= offsets.len() {
            offsets.resize(field_id + 1, usize::MAX);
        }

        // write persistently to mmap
        let mut mmap = &mut self.memory_map[..];
        let write_offset = mmap.len();

        match field.value_len() {
            Some(value_len) => {
                // static size
                mmap[write_offset..write_offset + value.len()].copy_from_slice(value);
            }
            None => {
                // dynamic size
                let value_len = (value.len() as u32).to_le_bytes();
                mmap[write_offset..write_offset + 4].copy_from_slice(&value_len[..]);
                mmap[write_offset + 4..write_offset + 4 + value.len()].copy_from_slice(value);
            }
        }
        mmap.flush()?;

        // update offset in index
        offsets[field_id] = write_offset;

        // persist hashmap index to file
        // [key-bytes-len as u32][key-bytes][field_id as u32][write_offset as u32]
        self.index_file.write(&(key.len() as u32).to_le_bytes())?;
        self.index_file.write(&key)?;
        self.index_file.write(&(field_id as u32).to_le_bytes())?;
        self.index_file
            .write(&(write_offset as u32).to_le_bytes())?;

        Ok(())
    }
}
