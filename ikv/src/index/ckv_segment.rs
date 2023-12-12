use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, BufReader, BufWriter, ErrorKind, Read, Seek, Write},
    iter,
    ops::DerefMut,
    path::Path,
};

use memmap2::MmapMut;
use protobuf::Message;

use crate::{
    proto::{
        self,
        generated_proto::index::{
            offset_table_entry, CKVIndexSegmentMetadata, DeleteDoc, DeleteDocFields,
            OffsetTableEntry, UpdateDocFields,
        },
    },
    schema::field::{Field, IndexedValue},
};

const CHUNK_SIZE: usize = 8 * 1024 * 1024; // 8M
const ZERO_I32: [u8; 4] = 0i32.to_le_bytes();

pub struct CKVIndexSegment {
    mount_directory: String,

    // hash-table index, document_id bytes -> vector of offsets
    // offsets point into the memory map
    offset_table_file_writer: BufWriter<File>,
    offset_table: HashMap<Vec<u8>, Vec<usize>>,

    // current (usable) offset for new writes into the mmap
    write_offset: usize,

    // metadata file writer
    metadata_file_writer: BufWriter<File>,

    // memory-mapping
    // underlying file, grows in chunks of size `CHUNK_SIZE`
    mmap_file: File,
    mmap: MmapMut,
}

impl CKVIndexSegment {
    /// Creates a brand new empty instance of a primary-key index.
    fn new(mount_directory: &str, index_id: usize) -> anyhow::Result<CKVIndexSegment> {
        // offset table index
        let offset_table_file = create_new_offset_table_file(mount_directory, index_id)?;

        // mmap file
        let mmap_file = create_new_mmap_file(mount_directory, index_id)?;
        let mmap = unsafe { MmapMut::map_mut(&mmap_file)? };

        // metadata file
        let mut metadata_file_writer =
            BufWriter::new(create_new_metadata_file(mount_directory, index_id)?);
        write_metadata(&mut metadata_file_writer, 0u64)?;

        Ok(CKVIndexSegment {
            mount_directory: mount_directory.to_string(),
            offset_table_file_writer: BufWriter::new(offset_table_file),
            offset_table: HashMap::new(),
            write_offset: 0 as usize,
            metadata_file_writer,
            mmap_file,
            mmap,
        })
    }

    pub fn open_or_create(
        mount_directory: &str,
        index_id: usize,
    ) -> anyhow::Result<CKVIndexSegment> {
        // offset-table
        let filename = format!(
            "{}/index/segment_{}/offset_table",
            mount_directory, index_id
        );
        if !Path::new(&filename).exists() {
            return CKVIndexSegment::new(mount_directory, index_id);
        }

        // offset-table exists on disk...
        let offset_table_file = open_offset_table_file(mount_directory, index_id)?;

        // build offset-table
        let mut offset_table = HashMap::new();
        let mut reader = BufReader::new(&offset_table_file);

        let mut entry_buffer = vec![];

        loop {
            // read size of serialized `OffsetTableEntry.proto`
            let entry_size: usize;
            let mut entry_size_buffer = vec![0u8; 4];
            match reader.read_exact(&mut entry_size_buffer) {
                Ok(_) => {
                    entry_size = i32::from_le_bytes(entry_size_buffer.try_into().unwrap()) as usize
                }
                Err(e) => {
                    match e.kind() {
                        ErrorKind::UnexpectedEof => {
                            // EOF reached
                            break;
                        }
                        _ => return Err(e.into()),
                    }
                }
            }

            // deserialize `OffsetTableEntry.proto` into offset_table
            if entry_buffer.len() < entry_size {
                let x = entry_size - entry_buffer.len();
                entry_buffer.extend(iter::repeat(0u8).take(x));
            }
            reader.read_exact(&mut entry_buffer[..entry_size])?;

            let entry = OffsetTableEntry::parse_from_bytes(&entry_buffer[..entry_size])?;
            if let Some(operation) = entry.operation {
                match operation {
                    offset_table_entry::Operation::UpdateDocFields(e) => {
                        let primary_key = &e.primary_key;
                        let offsets: &mut Vec<usize> =
                            offset_table.entry(primary_key.clone()).or_default();
                        for i in 0..e.field_ids.len() {
                            let field_id = *(&e.field_ids[i]) as usize;
                            let offset = *(&e.offsets[i]) as usize;

                            if field_id >= offsets.len() {
                                // needs expansion
                                offsets.resize(field_id + 1, usize::MAX);
                            }
                            offsets[field_id] = offset;
                        }
                    }
                    offset_table_entry::Operation::DeleteDocFields(e) => {
                        let primary_key = &e.primary_key;
                        if let Some(offsets) = offset_table.get_mut(primary_key) {
                            for field_id in e.field_ids.iter() {
                                let i = *field_id as usize;
                                if offsets.get(i).is_some() {
                                    offsets[i] = usize::MAX;
                                }
                            }
                        }
                    }
                    offset_table_entry::Operation::DeleteDoc(e) => {
                        let primary_key = &e.primary_key;
                        offset_table.remove(primary_key);
                    }
                }
            }
        }

        // mmap file
        let mmap_file = open_mmap_file(mount_directory, index_id)?;
        let mmap = unsafe { MmapMut::map_mut(&mmap_file)? };

        // metadata file
        let metadata_file = open_metadata_file(mount_directory, index_id)?;

        let write_offset: usize;
        {
            let mut metadata_file_reader = BufReader::new(metadata_file.try_clone()?);
            let mut buffer = Vec::new();
            metadata_file_reader.read_to_end(&mut buffer)?;
            let metadata = CKVIndexSegmentMetadata::parse_from_bytes(&buffer)?;
            write_offset = metadata.mmap_write_offset as usize;
            metadata_file_reader.rewind()?;
        }

        Ok(CKVIndexSegment {
            mount_directory: mount_directory.to_string(),
            offset_table_file_writer: BufWriter::new(offset_table_file),
            offset_table,
            write_offset,
            metadata_file_writer: BufWriter::new(metadata_file),
            mmap_file,
            mmap,
        })
    }

    /// Offline index build hook.
    /// Does field colocation and deletes compaction to create a compressed
    /// and efficient offline index.
    pub fn export(&mut self) -> anyhow::Result<()> {
        self.flush_writes()?;

        // TODO: add linear pass to compact.

        // The current index files on disk have been mutated
        // with ETL'ed kafka events.

        Ok(())
    }

    pub fn read_field(&self, primary_key: &[u8], field: &Field) -> Option<Vec<u8>> {
        let offsets = self.offset_table.get(primary_key)?;

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
        let maybe_offsets = self.offset_table.get(primary_key);
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

    /// Hook to persist incremental writes to disk
    /// ie parts of index and mmap files or schema
    /// Implementation is free to flush and write to disk
    /// upon each write_* invocation too.
    pub fn flush_writes(&mut self) -> io::Result<()> {
        // TODO: inspect if this needs to be a transaction and
        // what ordering we need.

        self.mmap.flush()?;

        write_metadata(&mut self.metadata_file_writer, self.write_offset as u64)?;

        self.offset_table_file_writer.flush()?;

        Ok(())
    }

    /// Upsert field values for a document.
    pub fn upsert_document(
        &mut self,
        primary_key: &[u8],
        fields: Vec<&Field>,
        field_values: Vec<IndexedValue>,
    ) -> io::Result<()> {
        {
            // mmap instantiation
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
        }
        let mmap = self.mmap.deref_mut();

        // propagate to disk (OffsetTableEntry.proto)
        let mut update_doc_fields = UpdateDocFields::new();
        update_doc_fields.primary_key = primary_key.to_vec();
        update_doc_fields.field_ids = Vec::with_capacity(fields.len());
        update_doc_fields.offsets = Vec::with_capacity(fields.len());

        let offsets = self.offset_table.entry(primary_key.to_vec()).or_default();

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

            // propagate to disk (OffsetTableEntry.proto)
            update_doc_fields.field_ids.push(field_id as i32);
            update_doc_fields.offsets.push(write_offset as i64);

            // update global offset
            self.write_offset += num_bytes;
        }

        let mut offset_table_entry = OffsetTableEntry::new();
        offset_table_entry.operation = Some(
            proto::generated_proto::index::offset_table_entry::Operation::UpdateDocFields(
                update_doc_fields,
            ),
        );
        self.persist_offset_table_update(offset_table_entry)
    }

    /// Delete field values for a document.
    pub fn delete_field_values(
        &mut self,
        primary_key: &[u8],
        fields: Vec<&Field>,
    ) -> io::Result<()> {
        if primary_key.len() == 0 || fields.len() == 0 {
            return Ok(());
        }

        // remove from in-memory offset_table
        let maybe_offsets = self.offset_table.get_mut(primary_key);
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

        // propagate to disk (OffsetTableEntry.proto)
        let mut delete_doc_fields = DeleteDocFields::new();
        delete_doc_fields.primary_key = primary_key.to_vec();
        delete_doc_fields.field_ids = Vec::with_capacity(fields.len());
        for field in fields.iter() {
            let field_id = field.id() as i32;
            delete_doc_fields.field_ids.push(field_id);
        }
        let mut offset_table_entry = OffsetTableEntry::new();
        offset_table_entry.operation = Some(
            proto::generated_proto::index::offset_table_entry::Operation::DeleteDocFields(
                delete_doc_fields,
            ),
        );
        self.persist_offset_table_update(offset_table_entry)
    }

    /// Delete document.
    pub fn delete_document(&mut self, primary_key: &[u8]) -> io::Result<()> {
        if primary_key.len() == 0 {
            return Ok(());
        }

        // remove from in-memory offset_table
        self.offset_table.remove(primary_key);

        // propagate to disk (OffsetTableEntry.proto)
        let mut delete_doc = DeleteDoc::new();
        delete_doc.primary_key = primary_key.to_vec();
        let mut offset_table_entry = OffsetTableEntry::new();
        offset_table_entry.operation = Some(
            proto::generated_proto::index::offset_table_entry::Operation::DeleteDoc(delete_doc),
        );
        self.persist_offset_table_update(offset_table_entry)
    }

    fn persist_offset_table_update(&mut self, entry: OffsetTableEntry) -> io::Result<()> {
        let bytes = entry.write_to_bytes()?;
        let size = bytes.len() as i32;
        self.offset_table_file_writer.write(&size.to_le_bytes())?;
        self.offset_table_file_writer.write(&bytes)?;

        Ok(())
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

        self.mmap.flush()?;
        self.mmap_file
            .write_all(&vec![0 as u8; CHUNK_SIZE * num_chunks])?;
        self.mmap_file.flush()?;
        self.mmap = unsafe { MmapMut::map_mut(&self.mmap_file)? };

        Ok(())
    }
}

fn write_metadata(writer: &mut BufWriter<File>, write_offset: u64) -> io::Result<()> {
    let mut metadata = CKVIndexSegmentMetadata::new();
    metadata.mmap_write_offset = write_offset;
    let bytes = metadata.write_to_bytes()?;
    writer.rewind()?;
    writer.write_all(&bytes)?;
    writer.flush()?;
    Ok(())
}

fn create_new_offset_table_file(dir: &str, index_id: usize) -> io::Result<File> {
    let filename = format!("{}/index/segment_{}/offset_table", dir, index_id);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(filename)?;
    Ok(file)
}

fn open_offset_table_file(dir: &str, index_id: usize) -> io::Result<File> {
    let filename = format!("{}/index/segment_{}/offset_table", dir, index_id);
    let mut file = OpenOptions::new()
        .read(true)
        .append(true)
        .create(false)
        .open(filename)?;
    file.seek(io::SeekFrom::Start(0))?;
    Ok(file)
}

fn create_new_mmap_file(dir: &str, index_id: usize) -> io::Result<File> {
    let filename = format!("{}/index/segment_{}/mmap", dir, index_id);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(filename)?;
    Ok(file)
}

fn open_mmap_file(dir: &str, index_id: usize) -> io::Result<File> {
    let filename = format!("{}/index/segment_{}/mmap", dir, index_id);
    let mut file = OpenOptions::new()
        .read(true)
        .append(true)
        .create(false)
        .open(filename)?;
    file.seek(io::SeekFrom::Start(0))?;
    Ok(file)
}

fn create_new_metadata_file(dir: &str, index_id: usize) -> io::Result<File> {
    let filename = format!("{}/index/segment_{}/metadata", dir, index_id);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(filename)?;
    Ok(file)
}

fn open_metadata_file(dir: &str, index_id: usize) -> io::Result<File> {
    let filename = format!("{}/index/segment_{}/metadata", dir, index_id);
    let mut file = OpenOptions::new()
        .read(true)
        .append(true)
        .create(false)
        .open(filename)?;
    file.seek(io::SeekFrom::Start(0))?;
    Ok(file)
}
