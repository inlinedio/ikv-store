use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, ErrorKind, Read, Write},
};

use memmap2::MmapMut;

use crate::schema::field::Field;

pub struct PrimaryKeyIndex {
    index: HashMap<Vec<u8>, Vec<usize>>,

    index_file: File,

    memory_map: MmapMut,
}

impl PrimaryKeyIndex {
    /// Creates a brand new empty instance of a primary-key index.
    pub fn new(mount_directory: &str, index_id: usize) -> io::Result<PrimaryKeyIndex> {
        let index: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();

        // hashmap persistence file
        let filename = format!("{}/index_{}", mount_directory, index_id);
        let index_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(filename)?;

        // memmap file
        let filename = format!("{}/mmap_{}", mount_directory, index_id);
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
    pub fn open(mount_directory: &str, index_id: usize) -> io::Result<PrimaryKeyIndex> {
        let mut index = HashMap::new();

        // hashmap persistence file
        let filename = format!("{}/index_{}", mount_directory, index_id);
        let mut index_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(filename)?;

        // Recreate the hashmap
        // Entry format
        // [(u16) document_id-size][document_id-bytes][(u16) num-fields=2][(u16) fieldid0][(u64) offset0][fieldid1][offset1]
        loop {
            let document_id_size;
            let mut buffer = vec![0 as u8; 2];
            match index_file.read_exact(&mut buffer) {
                Ok(_) => document_id_size = u16::from_le_bytes(buffer.try_into().unwrap()),
                Err(e) => {
                    match e.kind() {
                        ErrorKind::Interrupted => {
                            // EOF reached
                            break;
                        }
                        _ => return Err(e),
                    }
                }
            }

            let mut document_id = vec![0 as u8; document_id_size as usize];
            index_file.read_exact(&mut document_id)?;

            let num_fields = read_u16_from_file(&mut index_file);
            for _ in 0..num_fields {
                let field_id = read_u16_from_file(&mut index_file) as usize;
                let offset = read_u64_from_file(&mut index_file) as usize;

                // update index
                let offsets: &mut Vec<usize> = index.entry(document_id.to_vec()).or_default();
                if field_id as usize >= offsets.len() {
                    offsets.resize(field_id + 1, usize::MAX);
                }
                offsets[field_id] = offset;
            }
        }

        // memmap file
        let filename = format!("{}/mmap_{}", mount_directory, index_id);
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
    pub fn read(&self, document_id: &[u8], field: &Field) -> Option<Vec<u8>> {
        let offsets = self.index.get(document_id)?;
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
    pub fn upsert(
        &mut self,
        document_id: &[u8],
        field_value: &[u8],
        field: &Field,
    ) -> io::Result<()> {
        // write persistently to mmap
        let mut mmap = &mut self.memory_map[..];
        let write_offset = mmap.len();

        match field.value_len() {
            Some(value_len) => {
                // static size
                mmap[write_offset..write_offset + field_value.len()].copy_from_slice(field_value);
            }
            None => {
                // dynamic size
                let value_len = (field_value.len() as u32).to_le_bytes();
                mmap[write_offset..write_offset + 4].copy_from_slice(&value_len[..]);
                mmap[write_offset + 4..write_offset + 4 + field_value.len()]
                    .copy_from_slice(field_value);
            }
        }
        mmap.flush()?;

        // update offset in index
        let field_id = field.id() as usize;

        let offsets = self.index.entry(document_id.to_vec()).or_default();
        if field_id as usize >= offsets.len() {
            offsets.resize(field_id + 1, usize::MAX);
        }
        offsets[field_id] = write_offset;

        // persist hashmap index to file
        // Entry format
        // [(u16) document_id-size][document_id-bytes][(u16) num-fields=2][(u16) fieldid0][(u64) offset0][fieldid1][offset1]
        // document_id
        self.index_file
            .write(&(document_id.len() as u16).to_le_bytes())?;
        self.index_file.write(&document_id)?;
        // num fields=1
        self.index_file.write(&1u16.to_le_bytes())?;
        // field_id
        self.index_file.write(&(field_id as u16).to_le_bytes())?;
        // offset
        self.index_file
            .write(&(write_offset as u64).to_le_bytes())?;

        Ok(())
    }
}

fn read_u16_from_file(file: &mut File) -> u16 {
    let mut buffer = vec![0 as u8; 2];
    file.read_exact(&mut buffer).unwrap();
    u16::from_le_bytes(buffer.try_into().unwrap())
}

fn read_u64_from_file(file: &mut File) -> u64 {
    let mut buffer = vec![0 as u8; 8];
    file.read_exact(&mut buffer).unwrap();
    u64::from_le_bytes(buffer.try_into().unwrap())
}
