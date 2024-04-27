use crate::{
    proto::generated_proto::{
        common::FieldValue,
        common::{FieldType, IKVStoreConfig},
        index::CKVIndexHeader,
    },
    schema::field::FieldId,
};
use anyhow::{anyhow, bail};
use log::info;

use super::{
    ckv_segment::CKVIndexSegment, header::HeaderStore, offset_store::OffsetStore,
    schema_store::CKVIndexSchema, stats::CompactionStats,
};
use std::{
    collections::HashMap,
    fs::{self},
    path::Path,
    sync::RwLock,
};

#[cfg(test)]
#[path = "ckv_test.rs"]
mod ckv_test;

const NUM_SEGMENTS: usize = 16;

/// Memmap based row-oriented key-value index.
#[derive(Debug)]
pub struct CKVIndex {
    // Top level usable directory
    // Format: tmp/usr-mount-dir/<storename>/<partition>
    mount_directory: String,

    // hash(key) -> PrimaryKeyIndex
    segments: Vec<RwLock<CKVIndexSegment>>,

    /// field-name -> Field
    // Wrapped around lock to optimize for batch lookups
    schema: RwLock<CKVIndexSchema>,

    header_store: HeaderStore,
}

impl CKVIndex {
    pub fn open_or_create(config: &IKVStoreConfig) -> anyhow::Result<Self> {
        let mount_directory = crate::utils::paths::get_index_mount_directory_fqn(config)?;

        // create mount directory if it does not exist
        fs::create_dir_all(&mount_directory)?;

        // open_or_create schema
        let primary_key = config
            .stringConfigs
            .get("primary_key_field_name")
            .ok_or(anyhow!("primary_key is a required client-specified config"))?;

        let schema = CKVIndexSchema::open_or_create(&mount_directory, primary_key.clone())?;

        // open_or_create index segments
        let mut segments = Vec::with_capacity(NUM_SEGMENTS);
        for index_id in 0..NUM_SEGMENTS {
            let segment_mount_directory = format!("{}/index/segment_{}", mount_directory, index_id);

            // TODO: this needs to be parallelized, can take time for large indexes
            let segment = CKVIndexSegment::open_or_create(&segment_mount_directory)?;
            segments.push(RwLock::new(segment));
        }

        // open_or_create kafka store, done to initialize correctly
        let _ = OffsetStore::open_or_create(mount_directory.to_string())?;

        // index headers
        let header_store = HeaderStore::open_or_create(&mount_directory)?;

        Ok(Self {
            mount_directory,
            segments,
            schema: RwLock::new(schema),
            header_store,
        })
    }

    pub fn index_not_present(config: &IKVStoreConfig) -> anyhow::Result<bool> {
        let mount_directory = crate::utils::paths::get_index_mount_directory_fqn(config)?;
        let index_path = format!("{}/index", &mount_directory);

        let not_present = !Path::new(&index_path).exists()
            || CKVIndexSchema::index_not_present(&mount_directory)
            || OffsetStore::index_not_present(&mount_directory)
            || HeaderStore::index_not_present(&mount_directory);

        Ok(not_present)
    }

    // checks if a valid index is loaded at the mount directory
    // Returns error with some details if empty or invalid, else ok.
    // TODO: return bool wrapped in result.
    pub fn is_valid_index(config: &IKVStoreConfig) -> anyhow::Result<()> {
        let mount_directory = crate::utils::paths::get_index_mount_directory_fqn(config)?;

        // root path should exist
        let index_path = format!("{}/index", &mount_directory);
        if !Path::new(&index_path).exists() {
            bail!("IKVIndex root path does not exist at: {}", &index_path);
        }

        // check if all segments are valid
        for index_id in 0..NUM_SEGMENTS {
            let segment_mount_directory = format!("{}/index/segment_{}", mount_directory, index_id);
            CKVIndexSegment::is_valid_segment(&segment_mount_directory)?;
        }

        // check if schema is valid
        CKVIndexSchema::is_valid_index(&mount_directory)?;

        // check if kafka offset store is valid
        OffsetStore::is_valid_index(&mount_directory)?;

        // check if headers are valid
        HeaderStore::is_valid_index(&mount_directory)?;

        // valid index!
        Ok(())
    }

    /// Clears out all index structures from disk.
    pub fn delete_all(config: &IKVStoreConfig) -> anyhow::Result<()> {
        let mount_directory = crate::utils::paths::get_index_mount_directory_fqn(config)?;

        let index_path = format!("{}/index", &mount_directory);
        if Path::new(&index_path).exists() {
            std::fs::remove_dir_all(&index_path)?;
        }

        CKVIndexSchema::delete_all(&mount_directory)?;
        OffsetStore::delete_all(&mount_directory)?;
        HeaderStore::delete_all(&mount_directory)?;
        Ok(())
    }

    pub fn compact_and_close(mut self) -> anyhow::Result<(CompactionStats, CompactionStats)> {
        // schema compaction, get field id mapping
        let new_fid_to_old_fid = self.schema.write().unwrap().compact()?;

        // create (empty) compacted-segments
        let mut compacted_segments = Vec::with_capacity(NUM_SEGMENTS);
        for index_id in 0..NUM_SEGMENTS {
            let segment_mount_directory = format!(
                "{}/index/compacted_segment_{}",
                &self.mount_directory, index_id
            );
            let segment = CKVIndexSegment::open_or_create(&segment_mount_directory)?;
            compacted_segments.push(RwLock::new(segment));
        }

        // loop over existing segments, copy-to-compact, and close both
        let mut pre_compaction_stats: Vec<CompactionStats> = vec![];
        let mut post_compaction_stats: Vec<CompactionStats> = vec![];

        for (segment_id, segment) in self.segments.drain(..).enumerate() {
            info!(
                "Starting in-place compaction of index segment: {}",
                segment_id
            );

            let mut segment = segment.write().unwrap();
            let mut compacted_segment = compacted_segments[segment_id].write().unwrap();

            pre_compaction_stats.push(segment.compaction_stats()?);
            segment.copy_to_compact(&mut compacted_segment, &new_fid_to_old_fid)?;
            post_compaction_stats.push(compacted_segment.compaction_stats()?);
        }

        drop(compacted_segments);

        // swap directories
        for i in 0..NUM_SEGMENTS {
            let segment_mount_directory = format!("{}/index/segment_{}", &self.mount_directory, i);
            let compacted_segment_mount_directory =
                format!("{}/index/compacted_segment_{}", &self.mount_directory, i);
            std::fs::rename(&compacted_segment_mount_directory, &segment_mount_directory)?;
        }

        // print stats
        let pre_stats = CompactionStats::aggregate(&pre_compaction_stats);
        let post_stats = CompactionStats::aggregate(&post_compaction_stats);
        info!("Pre-compaction stats: {:?}", &pre_stats);
        info!("Post-compaction stats: {:?}", &post_stats);

        Ok((pre_stats, post_stats))
    }

    /// Fetch field value for a primary key.
    pub fn get_field_value(&self, primary_key: &[u8], field_name: &str) -> Option<Vec<u8>> {
        let field_id: FieldId;
        {
            let schema = self.schema.read().unwrap();
            field_id = schema.fetch_id_by_name(field_name)?;
        }

        let index_id: usize = fxhash::hash(primary_key) % NUM_SEGMENTS;
        let ckv_segment = self.segments[index_id].read().unwrap();

        ckv_segment.read_field(primary_key, field_id)
    }

    /// Fetch field values for multiple primary keys.
    /// Result format: [(values_doc1)][(values_doc2)][(values_doc3)]
    /// values_doc1: [(size)field1][(size)field2]...[(size)fieldn]
    /// size: 0 for empty values, size: -1 for missing values
    pub fn batch_get_field_values<'a>(
        &self,
        primary_keys: Vec<&[u8]>,
        field_names: Vec<&str>,
    ) -> Vec<u8> {
        let capacity = primary_keys.len() * field_names.len() * 16;
        if capacity == 0 {
            return vec![];
        }

        let mut result: Vec<u8> = Vec::with_capacity(capacity);

        // resolve field name strings to field_id(s)
        let mut field_ids: Vec<Option<FieldId>> = Vec::with_capacity(field_names.len());

        {
            let schema = self.schema.read().unwrap();
            for field_name in field_names {
                let maybe_field_id = schema.fetch_id_by_name(field_name);
                field_ids.push(maybe_field_id);
            }
        }

        // holds read acquired locks, released when we exit function scope
        let mut acquired_ckv_segments = Vec::with_capacity(NUM_SEGMENTS);
        for _ in 0..NUM_SEGMENTS {
            acquired_ckv_segments.push(None);
        }

        for primary_key in primary_keys.iter() {
            let index_id: usize = fxhash::hash(primary_key) % NUM_SEGMENTS;
            if acquired_ckv_segments[index_id].is_none() {
                acquired_ckv_segments[index_id] = Some(self.segments[index_id].read().unwrap());
            }

            let ckv_segment = acquired_ckv_segments[index_id].as_ref().unwrap();
            ckv_segment.read_fields(primary_key, &field_ids, &mut result);
        }

        result
    }

    /// Write APIs
    /// 1. upsert multiple fields for a document
    /// 2. delete multiple fields for a document
    /// 3. delete a document

    // full update - caller should provide all values.

    pub fn write_index_header(&self, header: &CKVIndexHeader) -> anyhow::Result<()> {
        self.header_store.write_header(header)
    }

    pub fn read_index_header(&self) -> anyhow::Result<CKVIndexHeader> {
        self.header_store.read_header()
    }

    /// Hook to persist incremental writes to disk
    /// ie parts of index and mmap files or schema
    /// Implementation is free to flush and write to disk
    /// upon each write_* invocation too.
    pub fn flush_writes(&self) -> anyhow::Result<()> {
        for segment in self.segments.iter() {
            let mut ckv_segment = segment.write().unwrap();
            ckv_segment.flush_writes()?;
        }

        Ok(())
    }

    pub fn upsert_field_values(
        &self,
        document: &HashMap<String, FieldValue>,
    ) -> anyhow::Result<()> {
        if document.is_empty() {
            return Ok(());
        }

        // upsert schema
        self.upsert_schema(document)?;

        // extract primary key
        let primary_key = self
            .extract_primary_key(document)?
            .ok_or(anyhow!("Cannot upsert with missing primary-key"))?;
        if primary_key.len() > u16::MAX as usize {
            bail!("primary_key larger than 64KB is unsupported");
        }

        // flatten to vectors
        let mut field_ids = Vec::with_capacity(document.len());
        let mut values = Vec::with_capacity(document.len());
        {
            let schema = self.schema.read().unwrap();
            for (field_name, field_value) in document.iter() {
                // Filter out unknown field types
                if field_value.fieldType.enum_value_or_default() == FieldType::UNKNOWN {
                    continue;
                }

                field_ids.push(
                    schema
                        .fetch_id_by_name(field_name)
                        .expect("upsert_schema ensures schema is known"),
                );
                values.push(field_value);
            }
        }

        if field_ids.is_empty() {
            // can occur when only unknown field types were inserted
            return Ok(());
        }

        let index_id = fxhash::hash(&primary_key) % NUM_SEGMENTS;
        let mut ckv_index_segment = self.segments[index_id].write().unwrap();
        ckv_index_segment.upsert_document(&primary_key, &field_ids, &values)?;
        Ok(())
    }

    // Note: deleting pkey from document is ok.
    pub fn delete_field_values(
        &self,
        document: &HashMap<String, FieldValue>,
        field_names: &[String],
    ) -> anyhow::Result<()> {
        if document.is_empty() || field_names.is_empty() {
            return Ok(());
        }

        // no schema upserts - we ignore unknown field names

        // extract primary key
        let primary_key = self
            .extract_primary_key(document)?
            .ok_or(anyhow!("Cannot delete with missing primary-key"))?;

        // flatten to vectors
        let mut field_ids = Vec::with_capacity(field_names.len());
        {
            let schema = self.schema.read().unwrap();
            for field_name in field_names {
                if let Some(field_id) = schema.fetch_id_by_name(field_name) {
                    field_ids.push(field_id);
                }
            }
        }

        let index_id = fxhash::hash(&primary_key) % NUM_SEGMENTS;
        let mut ckv_index_segment = self.segments[index_id].write().unwrap();
        ckv_index_segment.delete_field_values(&primary_key, &field_ids)?;

        Ok(())
    }

    /// Delete a document, given its primary key.
    pub fn delete_document(&self, document: &HashMap<String, FieldValue>) -> anyhow::Result<()> {
        if document.is_empty() {
            return Ok(());
        }

        // no schema upserts - we ignore unknown field names

        // extract primary key
        let primary_key = self
            .extract_primary_key(document)?
            .ok_or(anyhow!("Cannot delete with missing primary-key"))?;

        let index_id = fxhash::hash(&primary_key) % NUM_SEGMENTS;
        let mut ckv_index_segment = self.segments[index_id].write().unwrap();
        ckv_index_segment.delete_document(&primary_key)?;

        Ok(())
    }

    /// Drops provided fields (exact names or prefixes). Ignores attempt to drop primary-key.
    pub fn drop_fields(
        &self,
        field_names: &[String],
        field_name_prefixes: &[String],
    ) -> anyhow::Result<()> {
        if field_names.is_empty() && field_name_prefixes.is_empty() {
            return Ok(());
        }

        let mut schema = self.schema.write().unwrap();
        schema.soft_delete_fields(field_names, field_name_prefixes)
    }

    /// Drops all indexed data, and clears schema.
    pub fn drop_all_documents(&self) -> anyhow::Result<()> {
        for segment in self.segments.iter() {
            // ok to delete sequentially with reads to other segments
            // ok to fail partially
            segment.write().unwrap().delete_all_documents()?;
        }

        // clear schema, except primary-key
        let mut schema = self.schema.write().unwrap();
        schema.hard_delete_all_fields()
    }

    fn upsert_schema(&self, document: &HashMap<String, FieldValue>) -> anyhow::Result<()> {
        let mut needs_update = false;
        {
            let schema = self.schema.read().unwrap();
            for field_name in document.keys() {
                if schema.fetch_id_by_name(field_name).is_none() {
                    needs_update = true;
                    break;
                }
            }
        }

        if needs_update {
            let mut schema = self.schema.write().unwrap();
            schema.upsert_schema(document)?;
        }

        Ok(())
    }

    fn extract_primary_key(
        &self,
        document: &HashMap<String, FieldValue>,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        if document.is_empty() {
            return Ok(None);
        }

        let schema = self.schema.read().unwrap();

        // extract primary key
        let maybe_primary_key = schema.extract_primary_key_value(document);
        if maybe_primary_key.is_none() {
            return Ok(None);
        }

        let primary_key = maybe_primary_key.unwrap();

        if primary_key.fieldType.enum_value_or_default() == FieldType::UNKNOWN {
            bail!("Unsupported primary key type");
        }

        let serialized_primary_key = primary_key.value.clone();
        Ok(Some(serialized_primary_key))
    }
}
