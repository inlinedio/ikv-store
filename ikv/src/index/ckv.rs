use anyhow::{anyhow, bail};

use crate::{
    index::ckv_segment,
    proto::generated_proto::{
        common::FieldValue,
        common::{FieldSchema, IKVStoreConfig},
    },
    schema::{
        ckvindex_schema::CKVIndexSchema,
        field::{Field, IndexedValue},
    },
};

use super::ckv_segment::CKVIndexSegment;
use std::{
    collections::HashMap,
    fs::{self},
    io::{self, Error, ErrorKind},
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
    pub fn open_or_create(
        mount_directory: String,
        config: &IKVStoreConfig,
    ) -> anyhow::Result<Self> {
        // open_or_create saved schema
        let primary_key = config.stringConfigs.get("primary_key").ok_or(Error::new(
            ErrorKind::InvalidInput,
            "primary_key is a required config",
        ))?;
        let schema = CKVIndexSchema::open_or_create(&mount_directory, primary_key.clone())?;

        // ensure mount_directory exists
        fs::create_dir_all(mount_directory.clone())?;

        // TODO: inspect if we need to load a new base index!!!
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

    pub fn close(&self) -> io::Result<()> {
        // no op
        Ok(())
    }

    pub fn update_schema(&self, fields: &[FieldSchema]) -> anyhow::Result<()> {
        if fields.len() == 0 {
            return Ok(());
        }

        // check if needs update
        let mut needs_update = false;
        {
            let schema = self.schema.read().unwrap();
            for field in fields {
                if schema.fetch_field_by_name(&field.name).is_none() {
                    needs_update = true;
                    break;
                }
            }
        }

        if !needs_update {
            return Ok(());
        }

        let mut schema = self.schema.write().unwrap();
        schema.update(fields)
    }

    /// Fetch field value for a primary key.
    pub fn get_field_value(&self, primary_key: &[u8], field_name: &str) -> Option<Vec<u8>> {
        let schema = self.schema.read().unwrap();
        let field = schema.fetch_field_by_name(field_name)?;

        let index_id: usize = fxhash::hash(primary_key) % NUM_SEGMENTS;
        let ckv_segment = self.segments[index_id].read().unwrap();

        ckv_segment.read_field(primary_key, field)
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

        // resolve field name strings to &Field
        let schema = self.schema.read().unwrap();
        let mut fields: Vec<&Field> = Vec::with_capacity(field_names.len());
        for field_name in field_names {
            let field = schema
                .fetch_field_by_name(&field_name)
                .expect("cannot handle unknown field_names");
            fields.push(field);
        }
        let fields = fields.as_slice();

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
            ckv_segment.read_fields(primary_key, fields, &mut result);
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
        if document.len() == 0 {
            return Ok(());
        }

        // ensure we know schema of each field
        self.has_known_fields(document)?;

        // extract primary key
        let primary_key = self
            .extract_primary_key(document)
            .ok_or(anyhow!("Cannot upsert with missing primary-key"))?;
        if primary_key.len() > u16::MAX as usize {
            bail!("primary_key larger than 64KB is unsupported");
        }

        let schema = self.schema.read().unwrap();

        // flatten to vectors
        let mut fields = Vec::with_capacity(document.len());
        let mut values = Vec::with_capacity(document.len());
        for (field_name, field_value) in document.iter() {
            fields.push(
                schema
                    .fetch_field_by_name(field_name)
                    .expect("has_known_fields ensures schema is known"),
            );
            values.push(
                TryInto::<IndexedValue>::try_into(field_value)
                    .expect("has_known_fields ensures schema is known"),
            );
        }

        let index_id = fxhash::hash(&primary_key) % NUM_SEGMENTS;
        let mut ckv_index_segment = self.segments[index_id].write().unwrap();
        ckv_index_segment.upsert_document(&primary_key, fields, values)?;
        Ok(())
    }

    pub fn delete_field_values(
        &self,
        document: &HashMap<String, FieldValue>,
        field_names: &[String],
    ) -> anyhow::Result<()> {
        if document.len() == 0 || field_names.len() == 0 {
            return Ok(());
        }

        // ensure we know schema of each field
        self.has_known_fields(document)?;

        // extract primary key
        let primary_key = self
            .extract_primary_key(document)
            .ok_or(anyhow!("Cannot delete with missing primary-key"))?;

        let schema = self.schema.read().unwrap();

        // flatten to vectors
        let mut fields = Vec::with_capacity(document.len());
        for field_name in field_names.iter() {
            fields.push(
                schema
                    .fetch_field_by_name(field_name)
                    .expect("no new fields expected"), // TODO: there can be a mismatch b/w rust and proto enums
            );
        }

        let index_id = fxhash::hash(&primary_key) % NUM_SEGMENTS;
        let mut ckv_index_segment = self.segments[index_id].write().unwrap();
        ckv_index_segment.delete_field_values(&primary_key, fields)?;

        Ok(())
    }

    /// Delete a document, given its primary key.
    pub fn delete_document(&self, document: &HashMap<String, FieldValue>) -> anyhow::Result<()> {
        if document.len() == 0 {
            return Ok(());
        }

        // ensure we know schema of each field
        self.has_known_fields(document)?;

        // extract primary key
        let primary_key = self
            .extract_primary_key(document)
            .ok_or(anyhow!("Cannot delete with missing primary-key"))?;

        let index_id = fxhash::hash(&primary_key) % NUM_SEGMENTS;
        let mut ckv_index_segment = self.segments[index_id].write().unwrap();
        ckv_index_segment.delete_document(&primary_key)?;

        Ok(())
    }

    // Checks if every field in the document has known schema.
    fn has_known_fields(&self, document: &HashMap<String, FieldValue>) -> anyhow::Result<()> {
        let schema = self.schema.read().unwrap();
        for fieldname in document.keys() {
            schema
                .fetch_field_by_name(fieldname)
                .ok_or(anyhow!("Schema not known for field: {}", fieldname))?;
        }

        Ok(())
    }

    fn extract_primary_key(&self, document: &HashMap<String, FieldValue>) -> Option<Vec<u8>> {
        if document.len() == 0 {
            return None;
        }

        let schema = self.schema.read().unwrap();

        // extract primary key
        let primary_key = schema.extract_primary_key(&document)?;

        match TryInto::<IndexedValue>::try_into(primary_key) {
            Ok(iv) => Some(iv.serialize()),
            Err(_) => None,
        }
    }
}
