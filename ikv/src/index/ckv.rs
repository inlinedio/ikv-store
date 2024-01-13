use anyhow::{anyhow, bail};

use crate::{
    proto::generated_proto::{
        common::FieldValue,
        common::{FieldType, IKVStoreConfig},
    },
    schema::{ckvindex_schema::CKVIndexSchema, field::FieldId},
};

use super::{ckv_segment::CKVIndexSegment, offset_store::OffsetStore};
use std::{
    collections::HashMap,
    fs::{self},
    path::Path,
    sync::RwLock,
};

const NUM_SEGMENTS: usize = 16;

/// Memmap based columnar key-value index.
pub struct CKVIndex {
    // hash(key) -> PrimaryKeyIndex
    segments: Vec<RwLock<CKVIndexSegment>>,

    /// field-name -> Field
    // Wrapped around lock to optimize for batch lookups
    schema: RwLock<CKVIndexSchema>,
}

impl CKVIndex {
    pub fn open_or_create(config: &IKVStoreConfig) -> anyhow::Result<Self> {
        let mount_directory = crate::utils::paths::create_mount_directory(config)?;

        // create mount directory if it does not exist
        fs::create_dir_all(&mount_directory)?;

        // open_or_create saved schema
        let primary_key = config
            .stringConfigs
            .get("primary_key_field_name")
            .ok_or(anyhow!("primary_key is a required client-specified config"))?;
        let schema = CKVIndexSchema::open_or_create(&mount_directory, primary_key.clone())?;

        // open_or_create index segments
        let mut segments = Vec::with_capacity(NUM_SEGMENTS);
        for index_id in 0..NUM_SEGMENTS {
            let segment = CKVIndexSegment::open_or_create(&mount_directory, index_id)?;
            segments.push(RwLock::new(segment));
        }

        Ok(Self {
            segments,
            schema: RwLock::new(schema),
        })
    }

    pub fn close(&self) -> anyhow::Result<()> {
        // NO OP
        Ok(())
    }

    pub fn is_empty_index(config: &IKVStoreConfig) -> anyhow::Result<bool> {
        let mount_directory = crate::utils::paths::create_mount_directory(config)?;
        let index_path = format!("{}/index", &mount_directory);
        Ok(!Path::new(&index_path).exists())
    }

    // checks if a valid index is loaded at the mount directory
    // Returns error with some details if empty or invalid, else ok.
    pub fn is_valid_index(config: &IKVStoreConfig) -> anyhow::Result<()> {
        let mount_directory = crate::utils::paths::create_mount_directory(config)?;

        // root path should exist
        let index_path = format!("{}/index", &mount_directory);
        if !Path::new(&index_path).exists() {
            bail!("IKVIndex root path does not exist at: {}", &index_path);
        }

        // check if all segments are valid
        for i in 0..NUM_SEGMENTS {
            CKVIndexSegment::is_valid_index(&mount_directory, i)?;
        }

        // check if schema is valid
        CKVIndexSchema::is_valid_index(&mount_directory)?;
        OffsetStore::is_valid_index(&mount_directory)?;

        // valid index!
        Ok(())
    }

    /// Clears out all index structures from disk.
    pub fn delete_all(config: &IKVStoreConfig) -> anyhow::Result<()> {
        let mount_directory = crate::utils::paths::create_mount_directory(config)?;
        let index_path = format!("{}/index", &mount_directory);
        if Path::new(&index_path).exists() {
            std::fs::remove_dir_all(&index_path)?;
        }
        CKVIndexSchema::delete_all(&mount_directory)?;
        OffsetStore::delete_all(&mount_directory)?;
        Ok(())
    }

    pub fn compact(&self) -> anyhow::Result<()> {
        // lock all
        let mut segments = Vec::with_capacity(NUM_SEGMENTS);
        for i in 0..NUM_SEGMENTS {
            segments.push(self.segments[i].write().unwrap());
        }

        for mut segment in segments {
            segment.compact()?;
        }

        Ok(())
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
    /// size: 0 for empty values
    pub fn batch_get_field_values<'a>(
        &self,
        primary_keys: Vec<Vec<u8>>,
        field_names: Vec<String>,
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
                let maybe_field_id = schema.fetch_id_by_name(&field_name);
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
                        .expect("upsert_field_values ensures schema is known"),
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
        ckv_index_segment.upsert_document(&primary_key, field_ids, values)?;
        Ok(())
    }

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
        ckv_index_segment.delete_field_values(&primary_key, field_ids)?;

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
