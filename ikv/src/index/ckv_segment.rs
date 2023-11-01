use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, ErrorKind, Read, Write},
    ops::DerefMut,
};

use memmap2::MmapMut;

use crate::schema::field::Field;

const CHUNK_SIZE: usize = 8 * 1024 * 1024; // 8M

pub struct CKVIndexSegment {
    // hash-table index, document_id bytes -> vector of offsets
    // offsets point into the memory map
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
    pub fn new(mount_directory: &str, index_id: usize) -> io::Result<CKVIndexSegment> {
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

    /// Re-open a previously created index.
    pub fn open(mount_directory: &str, index_id: usize) -> io::Result<CKVIndexSegment> {
        // hash table index
        let filename = format!("{}/index_{}", mount_directory, index_id);
        let mut index_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(false)
            .open(filename)?;
        let mut index = HashMap::new();

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
                        ErrorKind::UnexpectedEof => {
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

    /// Read bytes for a given key and field, and append to "dest" vector.
    /// Return length (non-zero) as a result iff the field's value exists, or 0.
    pub fn read(&self, document_id: &[u8], field: &Field, dest: &mut Vec<u8>) -> usize {
        let maybe_offsets = self.index.get(document_id);
        if maybe_offsets == None {
            return 0;
        }

        let maybe_offset = maybe_offsets.unwrap().get(field.id() as usize).copied();
        if maybe_offset == None || maybe_offset.unwrap() == usize::MAX {
            return 0;
        }

        let offset = maybe_offset.unwrap();
        let value_len;

        let value = match field.value_len() {
            Some(fixed_value_len) => {
                // fixed size
                value_len = fixed_value_len;
                &self.mmap[offset..offset + value_len]
            }
            None => {
                // dynamic size
                let value_len_bytes = &self.mmap[offset..offset + 4];
                value_len = u32::from_le_bytes(
                    value_len_bytes
                        .try_into()
                        .expect("persisted value_len must be 4 bytes in length"),
                ) as usize;
                &self.mmap[offset + 4..offset + 4 + value_len]
            }
        };

        // append value bytes to destination
        // this can resize the vector
        dest.extend_from_slice(&value);

        return value_len;
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

    /// Upsert value (bytes) for a given key.
    pub fn upsert(
        &mut self,
        document_id: &[u8],
        field_value: &[u8],
        field: &Field,
    ) -> io::Result<()> {
        let write_offset = self.write_offset;

        // calculate number of bytes to write
        let num_bytes_to_write: usize;
        match field.value_len() {
            Some(_) => {
                // fixed size
                num_bytes_to_write = field_value.len();
            }
            None => {
                // dynamic size
                num_bytes_to_write = 4 + field_value.len();
            }
        }

        self.expand_mmap_if_required(write_offset, num_bytes_to_write)?;

        // write persistently to mmap
        let mut mmap = self.mmap.deref_mut();

        match field.value_len() {
            Some(value_len) => {
                // fixed size
                mmap[write_offset..write_offset + value_len].copy_from_slice(field_value);
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

        // update global write offset
        self.write_offset += num_bytes_to_write;

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
