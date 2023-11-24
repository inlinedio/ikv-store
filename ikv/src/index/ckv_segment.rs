use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, ErrorKind, Read, Write},
    ops::DerefMut,
    path::Path,
};

use memmap2::MmapMut;

use crate::schema::field::{Field, IndexedValue};

const CHUNK_SIZE: usize = 8 * 1024 * 1024; // 8M
const ZERO_I32: [u8; 4] = 0i32.to_le_bytes();

pub struct CKVIndexSegment {
    // hash-table index, document_id bytes -> vector of offsets
    // offsets point into the memory map
    // TODO: use BufWriters here.
    index_file: File,
    index: HashMap<Vec<u8>, Vec<usize>>,

    // current (usable) offset for new writes into the mmap
    write_offset: usize,

    // memory-mapping
    // underlying file, grows in chunks of size `CHUNK_SIZE`
    mmap_file: File,
    mmap: MmapMut,
}

impl CKVIndexSegment {
    /// Creates a brand new empty instance of a primary-key index.
    fn new(mount_directory: &str, index_id: usize) -> io::Result<CKVIndexSegment> {
        // hash table index
        let filename = format!("{}/index_{}", mount_directory, index_id);
        let index_file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(filename)?;

        // memmap file
        let filename = format!("{}/mmap_{}", mount_directory, index_id);
        let mmap_file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(filename)?;

        let mmap = unsafe { MmapMut::map_mut(&mmap_file)? };

        Ok(CKVIndexSegment {
            index_file,
            index: HashMap::new(),
            write_offset: 0 as usize,
            mmap_file,
            mmap,
        })
    }

    pub fn open_or_create(mount_directory: &str, index_id: usize) -> io::Result<CKVIndexSegment> {
        // hash table index
        let filename = format!("{}/index_{}", mount_directory, index_id);
        if !Path::new(&filename).exists() {
            return CKVIndexSegment::new(mount_directory, index_id);
        }

        // index already exists on disk...
        let mut index_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(false)
            .open(filename)?;
        let mut index = HashMap::new();

        // Recreate the hashmap
        // Entry format
        // [(u16) document_id-size][document_id-bytes][(u16) num-fields=2][(u16) fieldid0][(u64) offset0][fieldid1][offset1]

        // Index file format:
        // Upsert:           [primary_key_value_len as u16][primary_key_value][num_fields as u16][field_id][u64 offset][...][...][...]
        // Deleted Field:    [primary_key_value_len as u16][primary_key_value][num_fields as u16][field_id][u64::MAX][...][...][...]
        // Deleted Document: [primary_key_value_len as u16][primary_key_value][num_fields as u16 = u16::MAX]

        loop {
            let primary_key_size;
            let mut buffer = vec![0 as u8; 2];
            match index_file.read_exact(&mut buffer) {
                Ok(_) => primary_key_size = u16::from_le_bytes(buffer.try_into().unwrap()),
                Err(e) => {
                    match e.kind() {
                        ErrorKind::UnexpectedEof => {
                            // EOF reached
                            break;
                        }
                        _ => return Err(e),
                    }
                }
            }

            let mut primary_key = vec![0 as u8; primary_key_size as usize];
            index_file.read_exact(&mut primary_key)?;

            let num_fields = read_u16_from_file(&mut index_file);

            if num_fields == u16::MAX {
                // document was deleted.
                index.remove(&primary_key);
                continue;
            }

            // field upserts or field deletes
            for _ in 0..num_fields {
                let field_id = read_u16_from_file(&mut index_file) as usize;
                let offset = read_u64_from_file(&mut index_file);

                // update index
                let offsets: &mut Vec<usize> = index.entry(primary_key.to_vec()).or_default();
                if field_id as usize >= offsets.len() {
                    offsets.resize(field_id + 1, usize::MAX);
                }

                if offset == u64::MAX {
                    // deleted field value
                    offsets[field_id] = usize::MAX;
                } else {
                    // non-empty field value
                    offsets[field_id] = offset as usize;
                }
            }
        }

        // memmap file
        let filename = format!("{}/mmap_{}", mount_directory, index_id);
        let mamp_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(false)
            .open(filename)?;
        let file_metadata = mamp_file.metadata()?;

        // TODO(pushkar): write_offset is always set to end of file, resulting in some internal fragmentation
        let write_offset = file_metadata.len() as usize;

        let mmap = unsafe { MmapMut::map_mut(&mamp_file)? };

        Ok(CKVIndexSegment {
            index_file,
            index,
            write_offset,
            mmap_file: mamp_file,
            mmap,
        })
    }

    pub fn close(&self) {
        // no op
    }

    pub fn read_field(&self, primary_key: &[u8], field: &Field) -> Option<Vec<u8>> {
        let offsets = self.index.get(primary_key)?;

        let maybe_offset = offsets.get(field.id() as usize).copied();
        if maybe_offset == None || maybe_offset.unwrap() == usize::MAX {
            return None;
        }

        let result = self.read_from_mmap(field, maybe_offset.unwrap());
        Some(result.to_vec())
    }

    /// Read all fields for a given primary-key and push the values at the end of `dest` vector.
    /// Values are size/length prefixed with i32 values. Size=0 for missing values.
    ///
    /// Format of dest: [(size)field1][(size)field2]...[(size)fieldn]
    pub fn read_fields(&self, primary_key: &[u8], fields: &[&Field], dest: &mut Vec<u8>) {
        let maybe_offsets = self.index.get(primary_key);
        if maybe_offsets.is_none() {
            for _ in 0..fields.len() {
                dest.extend(ZERO_I32);
            }
            return;
        }

        let offsets = maybe_offsets.unwrap();
        for field in fields {
            let maybe_offset = offsets.get(field.id() as usize).copied();
            if maybe_offset == None || maybe_offset.unwrap() == usize::MAX {
                dest.extend(ZERO_I32);
                continue;
            }
            let value = self.read_from_mmap(field, maybe_offset.unwrap());

            dest.extend((value.len() as i32).to_le_bytes());
            dest.extend_from_slice(value);
        }
    }

    fn read_from_mmap(&self, field: &Field, mmap_offset: usize) -> &[u8] {
        let value = match field.value_len() {
            Some(fixed_width) => {
                // fixed width
                &self.mmap[mmap_offset..mmap_offset + fixed_width]
            }
            None => {
                // dynamic size
                let value_len_bytes = &self.mmap[mmap_offset..mmap_offset + 4];
                let value_len = u32::from_le_bytes(
                    value_len_bytes
                        .try_into()
                        .expect("persisted value_len must be 4 bytes in length"),
                ) as usize;
                &self.mmap[mmap_offset + 4..mmap_offset + 4 + value_len]
            }
        };

        value
    }

    // See: https://stackoverflow.com/questions/28516996/how-to-create-and-write-to-memory-mapped-files
    fn expand_mmap_if_required(
        &mut self,
        write_offset: usize,
        num_bytes_to_write: usize,
    ) -> io::Result<()> {
        let end_offset = write_offset + num_bytes_to_write; // non-inclusive
                                                            // space [write_offset, end_offset) should be available

        if self.mmap.len() >= end_offset {
            return Ok(());
        }

        let num_chunks =
            (1.0 + ((end_offset - self.mmap.len()) as f64 / CHUNK_SIZE as f64)) as usize;
        assert!(num_chunks >= 1);

        println!(
            "Need to resize the mmap. curr_len: {} write_offset: {} end_offset: {} num_chunks: {}",
            self.mmap.len(),
            write_offset,
            end_offset,
            num_chunks
        );

        self.mmap_file
            .write_all(&vec![0 as u8; CHUNK_SIZE * num_chunks])?;
        self.mmap_file.flush()?;
        self.mmap = unsafe { MmapMut::map_mut(&self.mmap_file)? };

        Ok(())
    }

    /// Index file format:
    /// Upsert: [primary_key_value_len as u16][primary_key_value][num_fields as u16][field_id][u64 offset]][...][...][...]
    /// Deleted Field (field value is u64::MAX): [primary_key_value_len as u16][primary_key_value][num_fields as u16][field_id as u16][u64::MAX][...][...][...]
    /// Deleted Document: [primary_key_value_len as u16][primary_key_value][num_fields as u16 = u16::MAX]

    /// Upsert field values for a document.
    pub fn upsert_document(
        &mut self,
        primary_key: &[u8],
        fields: Vec<&Field>,
        field_values: Vec<IndexedValue>,
    ) -> io::Result<()> {
        let mut total_num_bytes: usize = 0;
        for i in 0..fields.len() {
            let field: &Field = fields[i];
            let field_value = &field_values[i];
            if let Some(fixed_size) = field.value_len() {
                total_num_bytes += fixed_size;
            } else {
                total_num_bytes += 4 + field_value.len();
            }
        }

        // mmap instantiation
        self.expand_mmap_if_required(self.write_offset, total_num_bytes)?;
        let mut mmap = self.mmap.deref_mut();

        let offsets = self.index.entry(primary_key.to_vec()).or_default();

        // Index file format
        // Upsert: [primary_key_value_len as u16][primary_key_value][num_fields as u16][field_id][u64 offset][...][...][...]
        self.index_file
            .write(&(primary_key.len() as u16).to_le_bytes())?;
        self.index_file.write(&primary_key)?;
        self.index_file
            .write(&(fields.len() as u16).to_le_bytes())?;

        for i in 0..fields.len() {
            let field = fields[i];
            let field_id = field.id() as usize;
            let field_value = &field_values[i];

            let write_offset = self.write_offset;

            // write to mmap
            let num_bytes = if let Some(fixed_size) = field.value_len() {
                mmap[write_offset..write_offset + fixed_size]
                    .copy_from_slice(field_value.serialized_ref());

                fixed_size
            } else {
                let value_len = (field_value.len() as u32).to_le_bytes();
                mmap[write_offset..write_offset + 4].copy_from_slice(&value_len[..]);
                mmap[write_offset + 4..write_offset + 4 + field_value.len()]
                    .copy_from_slice(field_value.serialized_ref());

                4 + field_value.len()
            };

            // write to in-memory index
            if field_id >= offsets.len() {
                offsets.resize(field_id + 1, usize::MAX);
            }
            offsets[field_id] = write_offset;

            // persisted index file
            self.index_file.write(&(field_id as u16).to_le_bytes())?;
            self.index_file
                .write(&(write_offset as u64).to_le_bytes())?;

            // update global offset
            self.write_offset += num_bytes;
        }
        mmap.flush()?;

        Ok(())
    }

    /// Delete field values for a document.
    pub fn delete_field_values(
        &mut self,
        primary_key: &[u8],
        fields: Vec<&Field>,
    ) -> io::Result<()> {
        if fields.len() == 0 {
            return Ok(());
        }

        let maybe_offsets = self.index.get_mut(primary_key);
        if maybe_offsets.is_none() {
            return Ok(());
        }
        let offsets = maybe_offsets.unwrap();

        for field in fields.iter() {
            let field_id = field.id() as usize;
            if offsets.get(field_id).is_some() {
                offsets[field_id] = usize::MAX;
            }
        }

        // update persisted index_file
        // Deleted Field: [primary_key_value_len as u16][primary_key_value][num_fields as u16][field_id][u64::MAX][...][...][...]
        self.index_file
            .write(&(primary_key.len() as u16).to_le_bytes())?;
        self.index_file.write(&primary_key)?;
        self.index_file
            .write(&(fields.len() as u16).to_le_bytes())?;
        for field in fields.iter() {
            let field_id = field.id() as u16;
            self.index_file.write(&field_id.to_le_bytes())?;
            self.index_file.write(&u64::MAX.to_le_bytes())?;
        }

        Ok(())
    }

    /// Delete document.
    pub fn delete_document(&mut self, primary_key: &[u8]) -> io::Result<()> {
        // drop entry from pkey-index
        self.index.remove(primary_key);

        // update persisted index_file
        // Deleted Document: [primary_key_value_len as u16][primary_key_value][num_fields as u16 = u16::MAX]
        self.index_file
            .write(&(primary_key.len() as u16).to_le_bytes())?;
        self.index_file.write(&primary_key)?;
        self.index_file.write(&u16::MAX.to_le_bytes())?;

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
