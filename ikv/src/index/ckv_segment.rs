use std::{
    borrow::Borrow,
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, BufReader, BufWriter, ErrorKind, Read, Seek, Write},
    iter,
    ops::DerefMut,
    path::Path,
};

use anyhow::bail;
use integer_encoding::VarInt;
use log::debug;
use memmap2::MmapMut;
use protobuf::{Enum, Message};

use crate::{
    proto::{
        self,
        generated_proto::{
            common::{FieldType, FieldValue},
            index::{
                offset_table_entry, CKVIndexSegmentMetadata, DeleteDoc, DeleteDocFields,
                OffsetTableEntry, UpdateDocFields,
            },
        },
    },
    schema::field::FieldId,
};

use super::stats::CompactionStats;

const CHUNK_SIZE: usize = 8 * 1024 * 1024; // 8M
const EMPTY_BYTE_SLICE: &[u8] = &[];
const NONE_SIZE: [u8; 4] = (-1 as i32).to_le_bytes();

#[derive(Debug)]
pub struct CKVIndexSegment {
    /**
     * file references
     */
    // offset-table-file: persists changes to offset table
    offset_table_file_writer: BufWriter<File>,

    // metadata-file: persists information about this segment like next available offset into mmap file
    metadata_file_writer: BufWriter<File>,

    // mmap file, stores payloads
    mmap_file: File,

    /**
     * Core index structures.
     */
    // hash-table index, document_id bytes -> vector of offsets
    // offsets point into the memory map
    // TODO: offset table should hold u64 for cross platform index builds
    // TODO: this is incompatible on 32 bit architecture
    offset_table: HashMap<Vec<u8>, Vec<usize>>,
    mmap: MmapMut,
    write_offset: u64,
}

impl CKVIndexSegment {
    /// segment_mount_directory format: usr-mount-dir/store/partition/index/segment-N
    pub fn open_or_create(segment_mount_directory: &str) -> anyhow::Result<CKVIndexSegment> {
        // check if `segment_mount_directory` is present - valid index or create one
        if !Path::new(segment_mount_directory).exists() {
            std::fs::create_dir_all(segment_mount_directory)?;
            return CKVIndexSegment::new(segment_mount_directory);
        }

        // offset-table exists on disk...
        let offset_table_file = open_offset_table_file(segment_mount_directory)?;

        // build offset-table by replaying offset_table_file
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
                            let field_id = e.field_ids[i] as usize;
                            let offset = e.offsets[i] as usize;

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
        let mmap_file = open_mmap_file(segment_mount_directory)?;
        let mmap = unsafe { MmapMut::map_mut(&mmap_file)? };

        // metadata file
        let metadata_file = open_metadata_file(segment_mount_directory)?;

        let write_offset: u64;
        {
            let mut metadata_file_reader = BufReader::new(metadata_file.try_clone()?);
            let mut buffer = Vec::new();
            metadata_file_reader.read_to_end(&mut buffer)?;
            let metadata = CKVIndexSegmentMetadata::parse_from_bytes(&buffer)?;
            write_offset = metadata.mmap_write_offset;
            metadata_file_reader.rewind()?;
        }

        Ok(CKVIndexSegment {
            offset_table_file_writer: BufWriter::new(offset_table_file),
            metadata_file_writer: BufWriter::new(metadata_file),
            mmap_file,
            offset_table,
            mmap,
            write_offset,
        })
    }

    /// Creates a brand new empty instance of a primary-key index.
    fn new(segment_mount_directory: &str) -> anyhow::Result<CKVIndexSegment> {
        // offset table index
        let offset_table_file = create_new_offset_table_file(segment_mount_directory)?;

        // mmap file
        let mmap_file = create_new_mmap_file(segment_mount_directory)?;
        let mmap = unsafe { MmapMut::map_mut(&mmap_file)? };

        // metadata file
        let mut metadata_file_writer =
            BufWriter::new(create_new_metadata_file(segment_mount_directory)?);
        write_metadata(&mut metadata_file_writer, 0u64)?;

        Ok(CKVIndexSegment {
            offset_table_file_writer: BufWriter::new(offset_table_file),
            metadata_file_writer,
            mmap_file,
            offset_table: HashMap::new(),
            mmap,
            write_offset: 0,
        })
    }

    pub fn is_valid_segment(segment_mount_directory: &str) -> anyhow::Result<()> {
        let filename = format!("{}/metadata", segment_mount_directory);
        if !Path::new(&filename).exists() {
            bail!(
                "CKVIndexSegment metadata file does not exist: {}",
                &filename
            );
        }

        let filename = format!("{}/mmap", segment_mount_directory);
        if !Path::new(&filename).exists() {
            bail!("CKVIndexSegment mmap file does not exist: {}", &filename);
        }

        let filename = format!("{}/offset_table", segment_mount_directory);
        if !Path::new(&filename).exists() {
            bail!(
                "CKVIndexSegment offset_table file does not exist: {}",
                &filename
            );
        }

        Ok(())
    }

    pub fn copy_to_compact(
        &mut self,
        destination: &mut CKVIndexSegment,
        new_fid_to_old_fid: &[FieldId],
    ) -> anyhow::Result<()> {
        self.flush_writes()?;

        for (primary_key, offsets) in self.offset_table.iter() {
            // construct document to copy
            let capacity = std::cmp::min(offsets.len(), new_fid_to_old_fid.len());
            let mut field_ids = Vec::with_capacity(capacity);
            let mut field_values = Vec::with_capacity(capacity);

            for new_fid in 0..new_fid_to_old_fid.len() {
                let old_fid = new_fid_to_old_fid[new_fid];
                if let Some(offset) = offsets.get(old_fid as usize).copied() {
                    if let Some((field_type, value)) = self.read_from_mmap(offset) {
                        if field_type == FieldType::UNKNOWN {
                            // either write event in kafka stream was missing type info, or
                            // this node is behind on symbol list
                            bail!(
                                "Found unknown fieldType for primary-key: {}",
                                format!("{:?}", primary_key.as_slice())
                            );
                        }

                        let mut field_value = FieldValue::new();
                        field_value.fieldType = field_type.into();
                        field_value.value = value.to_vec();
                        field_values.push(field_value);

                        field_ids.push(new_fid as FieldId);
                    }
                }
            }

            // write to destination segment
            destination.upsert_document(primary_key, &field_ids, &field_values)?;
        }

        destination.flush_writes()?;
        Ok(())
    }

    pub fn compaction_stats(&self) -> anyhow::Result<CompactionStats> {
        return Ok(CompactionStats {
            offset_table_size_bytes: self.offset_table_file_writer.get_ref().metadata()?.len(),
            mmap_file_size_bytes: self.mmap_file.metadata()?.len(),
        });
    }

    pub fn read_field(&self, primary_key: &[u8], field_id: FieldId) -> Option<Vec<u8>> {
        let offsets = self.offset_table.get(primary_key)?;
        let maybe_offset = offsets.get(field_id as usize).copied();
        if let Some(offset) = maybe_offset {
            let (field_type, result) = self.read_from_mmap(offset)?;
            if field_type != FieldType::UNKNOWN {
                return Some(result.to_vec());
            }
        }

        None
    }

    /// Read all fields for a given primary-key and push the values at the end of `dest` vector.
    /// Values are size/length prefixed with i32 values. Size=-1 for missing values, Size=0 for empty values
    ///
    /// Format of dest: [(size)field1][(size)field2]...[(size)fieldn]
    pub fn read_fields(
        &self,
        primary_key: &[u8],
        field_ids: &[Option<FieldId>],
        dest: &mut Vec<u8>,
    ) {
        let maybe_offsets = self.offset_table.get(primary_key);
        if maybe_offsets.is_none() {
            for _ in 0..field_ids.len() {
                dest.extend(NONE_SIZE);
            }
            return;
        }

        let offsets = maybe_offsets.unwrap();
        for field_id in field_ids {
            if field_id.is_none() {
                dest.extend(NONE_SIZE);
                continue;
            }

            let maybe_offset = offsets.get(field_id.unwrap() as usize).copied();
            if maybe_offset.is_none() {
                dest.extend(NONE_SIZE);
                continue;
            }

            match self.read_from_mmap(maybe_offset.unwrap()) {
                None => {
                    dest.extend(NONE_SIZE);
                }
                Some((field_type, value)) => {
                    if field_type == FieldType::UNKNOWN {
                        dest.extend(NONE_SIZE);
                    } else {
                        dest.extend((value.len() as i32).to_le_bytes());
                        dest.extend_from_slice(value);
                    }
                }
            };
        }
    }

    fn read_from_mmap(&self, mmap_offset: usize) -> Option<(FieldType, &[u8])> {
        if mmap_offset == usize::MAX {
            return None;
        }

        // mmap_offset points to a bytes section where:
        // [2 bytes for field-type][data]
        // where data can be prefixed with vbytes for variable length types

        let field_type_bytes = &self.mmap[mmap_offset..mmap_offset + 2];
        let field_type: u16 = u16::from_le_bytes(
            field_type_bytes
                .try_into()
                .expect("mmap value must be prefixed with 2 byte field types"),
        );
        let field_type: FieldType = FieldType::from_i32(i32::from(field_type))?;

        let mmap_offset = mmap_offset + 2;
        match field_type {
            FieldType::UNKNOWN => {
                // Some unknown field-type was written to the mmap files
                // can occur when this reader is behind on the FieldType.proto symbol list.
                // Can be okay to ignore this when doing live reads on the index, but this
                // should not be ignored during data copy (ex. compaction).
                Some((field_type, EMPTY_BYTE_SLICE))
            }
            FieldType::INT32 | FieldType::FLOAT32 => {
                Some((field_type, &self.mmap[mmap_offset..mmap_offset + 4]))
            }
            FieldType::INT64 | FieldType::FLOAT64 => {
                Some((field_type, &self.mmap[mmap_offset..mmap_offset + 8]))
            }
            FieldType::BOOLEAN => Some((field_type, &self.mmap[mmap_offset..mmap_offset + 1])),
            FieldType::STRING | FieldType::BYTES => {
                // extract size (varint decoding)
                let (size, bytes_read) = u32::decode_var(&self.mmap[mmap_offset..])?;
                let mmap_offset = mmap_offset + bytes_read;
                Some((
                    field_type,
                    &self.mmap[mmap_offset..mmap_offset + size as usize],
                ))
            }
        }
    }

    /// Hook to persist incremental writes to disk
    /// ie parts of index and mmap files or schema
    /// Implementation is free to flush and write to disk
    /// upon each write_* invocation too.
    pub fn flush_writes(&mut self) -> io::Result<()> {
        // TODO: inspect if this needs to be a transaction and
        // what ordering we need.

        // TODO: do we need this? self.mmap.flush()?;
        self.mmap_file.flush()?;

        write_metadata(&mut self.metadata_file_writer, self.write_offset as u64)?;

        self.offset_table_file_writer.flush()?;

        Ok(())
    }

    pub fn close(mut self) -> io::Result<()> {
        self.flush_writes()
    }

    fn size_of_mmap_entry(field_value: &FieldValue) -> anyhow::Result<usize> {
        if field_value.fieldType.value() > u16::MAX as i32 {
            bail!("Cannot store field type in 2 bytes");
        }
        let mut size: usize = 2; // for storing type

        match field_value.fieldType.enum_value_or_default() {
            FieldType::UNKNOWN => {
                // unknown types are serialized by just saving the type (=0)
                size += 0
            }
            FieldType::INT32 | FieldType::FLOAT32 => size += 4,
            FieldType::INT64 | FieldType::FLOAT64 => size += 8,
            FieldType::BOOLEAN => size += 1,
            FieldType::STRING | FieldType::BYTES => {
                // length varint + actual content
                let value_len = field_value.value.len();
                if value_len > u32::MAX as usize {
                    bail!("size of value cannot exceed 4GB");
                }

                let value_len_prefix = u32::required_space(value_len as u32);
                size += value_len_prefix + value_len
            }
        };

        Ok(size)
    }

    /// Write provided value to mmap at provided offset, returning the number of bytes written.
    fn write_to_mmap(
        mmap: &mut [u8],
        write_offset: usize,
        field_value: &FieldValue,
    ) -> anyhow::Result<usize> {
        if field_value.fieldType.value() > u16::MAX as i32 {
            bail!("Cannot store field type in 2 bytes");
        }

        // TODO: copy_from_slice() panics when src/dest slice lenghts are different.
        // Prevalidate lengths match and handle errors accordingly.

        // TODO: consider being more robust by not gettings stuck on incorrect input
        // events - limit blast radius of bad serialization schemes in producer.

        // serialize field_type
        if CKVIndexSegment::is_unknown_field_type(field_value) {
            // we save unknown types as sentinels
            // they are ignored during normal read ops but caught during compaction
            let field_type = (FieldType::UNKNOWN.value() as u16).to_le_bytes();
            mmap[write_offset..write_offset + 2].copy_from_slice(&field_type[..]);
        } else {
            let field_type = (field_value.fieldType.value() as u16).to_le_bytes();
            mmap[write_offset..write_offset + 2].copy_from_slice(&field_type[..]);
        }

        let mut num_bytes = 2;
        let write_offset = write_offset + 2;

        // write value
        num_bytes += match field_value.fieldType.enum_value_or_default() {
            FieldType::UNKNOWN => {
                // no field value data required
                0
            }
            FieldType::INT32 | FieldType::FLOAT32 => {
                mmap[write_offset..write_offset + 4].copy_from_slice(&field_value.value[..]);
                4
            }
            FieldType::INT64 | FieldType::FLOAT64 => {
                mmap[write_offset..write_offset + 8].copy_from_slice(&field_value.value[..]);
                8
            }
            FieldType::BOOLEAN => {
                mmap[write_offset..write_offset + 1].copy_from_slice(&field_value.value[..]);
                1
            }
            FieldType::STRING | FieldType::BYTES => {
                let value_len = field_value.value.len();

                // value length prefix
                let x = u32::encode_var(value_len as u32, &mut mmap[write_offset..]);

                // value
                mmap[write_offset + x..write_offset + x + value_len]
                    .copy_from_slice(&field_value.value[..]);

                x + value_len
            }
        };

        Ok(num_bytes)
    }

    /// Upsert field values for a document.
    pub fn upsert_document<T>(
        &mut self,
        primary_key: &[u8],
        field_ids: &[FieldId],
        field_values: &[T],
    ) -> anyhow::Result<()>
    where
        T: Borrow<FieldValue>,
    {
        if primary_key.is_empty() || field_ids.is_empty() {
            return Ok(());
        }

        // Unknown field types (i.e. FieldType::UNKNOWN) handling.
        // They can occur when we are behind on symbol list or upstream ingestion path didn't propelry construct the write event
        // We save a sentinel which is 2 bytes for the field-type (=0), and no additonal data.
        // They get ignored during normal reads, but get caught during compaction.
        //
        // TODO: we should have cfg flags to block kafka stream for such events (catch compaction errors earlier).

        // mmap resizing?
        let mmap;
        {
            // mmap instantiation
            let mut mmap_entry_size = 0;
            for field_value in field_values.iter().map(|fv| fv.borrow()) {
                mmap_entry_size += Self::size_of_mmap_entry(field_value)?;
            }

            // mmap instantiation
            self.expand_mmap_if_required(self.write_offset, mmap_entry_size)?;
            mmap = self.mmap.deref_mut();
        }

        // propagate to disk (OffsetTableEntry.proto)
        let mut update_doc_fields = UpdateDocFields::new();
        update_doc_fields.primary_key = primary_key.to_vec();
        update_doc_fields.field_ids = Vec::with_capacity(field_ids.len());
        update_doc_fields.offsets = Vec::with_capacity(field_ids.len());

        let offsets = self.offset_table.entry(primary_key.to_vec()).or_default();

        for i in 0..field_ids.len() {
            let field_id = field_ids[i];
            let field_value = field_values[i].borrow();

            let write_offset = self.write_offset;

            // write value to mmap
            let num_bytes = Self::write_to_mmap(mmap, write_offset as usize, field_value)?;

            // write to in-memory index
            if field_id >= offsets.len() as u32 {
                offsets.resize(field_id as usize + 1, usize::MAX);
            }
            offsets[field_id as usize] = write_offset as usize;

            // propagate to disk (OffsetTableEntry.proto)
            update_doc_fields.field_ids.push(field_id);
            update_doc_fields.offsets.push(write_offset as u64);

            // update global offset
            self.write_offset += num_bytes as u64;
        }

        // propagate to disk (OffsetTableEntry.proto)
        let mut offset_table_entry = OffsetTableEntry::new();
        offset_table_entry.operation = Some(
            proto::generated_proto::index::offset_table_entry::Operation::UpdateDocFields(
                update_doc_fields,
            ),
        );
        self.persist_offset_table_update(offset_table_entry)?;

        Ok(())
    }

    fn is_unknown_field_type(value: &FieldValue) -> bool {
        return value.fieldType.enum_value_or_default() == FieldType::UNKNOWN;
    }

    /// Delete field values for a document.
    pub fn delete_field_values(
        &mut self,
        primary_key: &[u8],
        field_ids: &[FieldId],
    ) -> io::Result<()> {
        if primary_key.is_empty() || field_ids.is_empty() {
            return Ok(());
        }

        // propagate to disk (OffsetTableEntry.proto)
        let mut delete_doc_fields = DeleteDocFields::new();
        delete_doc_fields.primary_key = primary_key.to_vec();
        delete_doc_fields.field_ids = Vec::with_capacity(field_ids.len());
        for field_id in field_ids.iter() {
            delete_doc_fields.field_ids.push(*field_id);
        }
        let mut offset_table_entry = OffsetTableEntry::new();
        offset_table_entry.operation = Some(
            proto::generated_proto::index::offset_table_entry::Operation::DeleteDocFields(
                delete_doc_fields,
            ),
        );
        self.persist_offset_table_update(offset_table_entry)?;

        // remove from in-memory offset_table
        let maybe_offsets = self.offset_table.get_mut(primary_key);
        if maybe_offsets.is_none() {
            return Ok(());
        }
        let offsets = maybe_offsets.unwrap();
        for field_id in field_ids.iter() {
            let field_id = *field_id as usize;
            if offsets.get(field_id).is_some() {
                offsets[field_id] = usize::MAX;
            }
        }

        Ok(())
    }

    /// Delete document (soft delete).
    pub fn delete_document(&mut self, primary_key: &[u8]) -> io::Result<()> {
        if primary_key.is_empty() {
            return Ok(());
        }

        // propagate to disk (OffsetTableEntry.proto)
        let mut delete_doc = DeleteDoc::new();
        delete_doc.primary_key = primary_key.to_vec();
        let mut offset_table_entry = OffsetTableEntry::new();
        offset_table_entry.operation = Some(
            proto::generated_proto::index::offset_table_entry::Operation::DeleteDoc(delete_doc),
        );
        self.persist_offset_table_update(offset_table_entry)?;

        // remove from in-memory offset_table
        self.offset_table.remove(primary_key);
        Ok(())
    }

    /// Drops all indexed data (hard delete).
    pub fn delete_all_documents(&mut self) -> io::Result<()> {
        // flush pending writes
        self.flush_writes()?;

        // clear offset table and file (deallocate memory too)
        self.offset_table_file_writer.flush()?;
        self.offset_table_file_writer.get_ref().set_len(0)?;
        self.offset_table_file_writer.rewind()?;
        self.offset_table = HashMap::new();

        // clear mmap
        self.mmap_file.set_len(0)?;
        self.mmap_file.rewind()?;
        self.mmap = unsafe { MmapMut::map_mut(&self.mmap_file)? };
        self.write_offset = 0;
        write_metadata(&mut self.metadata_file_writer, 0u64)?;

        Ok(())
    }

    fn persist_offset_table_update(&mut self, entry: OffsetTableEntry) -> io::Result<()> {
        let bytes = entry.write_to_bytes()?;
        let size = bytes.len() as i32;
        self.offset_table_file_writer
            .write_all(&size.to_le_bytes())?;
        self.offset_table_file_writer.write_all(&bytes)?;

        Ok(())
    }

    // See: https://stackoverflow.com/questions/28516996/how-to-create-and-write-to-memory-mapped-files
    fn expand_mmap_if_required(
        &mut self,
        write_offset: u64,
        num_bytes_to_write: usize,
    ) -> io::Result<()> {
        let end_offset = write_offset as usize + num_bytes_to_write; // non-inclusive
                                                                     // space [write_offset, end_offset) should be available

        if self.mmap.len() >= end_offset {
            return Ok(());
        }

        let num_chunks =
            (1.0 + ((end_offset - self.mmap.len()) as f64 / CHUNK_SIZE as f64)) as usize;
        assert!(num_chunks >= 1);

        debug!(
            "Need to resize the mmap. curr_len: {} write_offset: {} end_offset: {} num_chunks: {}",
            self.mmap.len(),
            write_offset,
            end_offset,
            num_chunks
        );

        // TODO: do we need this? self.mmap.flush().expect("cannot flush1");
        self.mmap_file
            .write_all(&vec![0_u8; CHUNK_SIZE * num_chunks])?;
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

fn create_new_offset_table_file(segment_mount_directory: &str) -> io::Result<File> {
    let filename = format!("{}/offset_table", segment_mount_directory);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(filename)?;
    Ok(file)
}

fn open_offset_table_file(segment_mount_directory: &str) -> io::Result<File> {
    let filename = format!("{}/offset_table", segment_mount_directory);
    let mut file = OpenOptions::new()
        .read(true)
        .append(true)
        .create(false)
        .open(filename)?;
    file.seek(io::SeekFrom::Start(0))?;
    Ok(file)
}

fn create_new_mmap_file(segment_mount_directory: &str) -> io::Result<File> {
    let filename = format!("{}/mmap", segment_mount_directory);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(filename)?;
    Ok(file)
}

fn open_mmap_file(segment_mount_directory: &str) -> io::Result<File> {
    let filename = format!("{}/mmap", segment_mount_directory);
    let mut file = OpenOptions::new()
        .read(true)
        .append(true)
        .create(false)
        .open(filename)?;
    file.seek(io::SeekFrom::Start(0))?;
    Ok(file)
}

fn create_new_metadata_file(segment_mount_directory: &str) -> io::Result<File> {
    let filename = format!("{}/metadata", segment_mount_directory);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(filename)?;
    Ok(file)
}

fn open_metadata_file(segment_mount_directory: &str) -> io::Result<File> {
    let filename = format!("{}/metadata", segment_mount_directory);
    let mut file = OpenOptions::new()
        .read(true)
        .append(true)
        .create(false)
        .open(filename)?;
    file.seek(io::SeekFrom::Start(0))?;
    Ok(file)
}
