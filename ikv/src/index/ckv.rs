use crate::{
    proto::generated_proto::{
        common::{FieldSchema, IKVStoreConfig},
        services::FieldValue,
    },
    schema::{
        self,
        ckvindex_schema::CKVIndexSchema,
        error::SchemaError,
        field::{Field, IndexedValue},
    },
};

use super::{ckv_segment::CKVIndexSegment, error::IndexError};
use std::{
    collections::HashMap,
    fs::{self},
    io::{self, Error, ErrorKind},
    sync::RwLock,
};

const NUM_SEGMENTS: usize = 16;

/// Memmap based columnar key-value index.
pub struct CKVIndex {
    mount_directory: String,

    // hash(key) -> PrimaryKeyIndex
    segments: Vec<RwLock<CKVIndexSegment>>,

    // field-name -> Field
    schema: RwLock<CKVIndexSchema>,
}

impl CKVIndex {
    #[deprecated]
    pub fn new(mount_directory: String, config: &IKVStoreConfig) -> io::Result<CKVIndex> {
        // ensure mount_directory exists
        fs::create_dir_all(mount_directory.clone())?;

        // create schema
        let primary_key = config.stringConfigs.get("primary_key").ok_or(Error::new(
            ErrorKind::InvalidInput,
            "primary_key is a required config",
        ))?;
        let schema = CKVIndexSchema::open_or_create(&mount_directory, primary_key.clone())?;

        // create index segments
        let mut segments = vec![];
        for index_id in 0..NUM_SEGMENTS {
            let segment = CKVIndexSegment::open_or_create(&mount_directory, index_id)?;
            segments.push(RwLock::new(segment));
        }

        Ok(Self {
            mount_directory,
            segments,
            schema: RwLock::new(schema),
        })
    }

    pub fn open(mount_directory: String, config: &IKVStoreConfig) -> io::Result<CKVIndex> {
        // load schema
        let primary_key = config.stringConfigs.get("primary_key").ok_or(Error::new(
            ErrorKind::InvalidInput,
            "primary_key is a required config",
        ))?;
        let schema = CKVIndexSchema::open_or_create(&mount_directory, primary_key.clone())?;

        // TODO: inspect if we need to load a new base index!!!
        // open index segments
        let mut segments = Vec::with_capacity(NUM_SEGMENTS);
        for index_id in 0..NUM_SEGMENTS {
            let segment = CKVIndexSegment::open_or_create(&mount_directory, index_id)?;
            segments.push(RwLock::new(segment));
        }

        Ok(Self {
            mount_directory,
            segments,
            schema: RwLock::new(schema),
        })
    }

    pub fn close(&self) -> io::Result<()> {
        // save schema
        self.schema.write().unwrap().save(&self.mount_directory)?;

        // close segments
        for segment in self.segments.iter() {
            segment.read().unwrap().close();
        }

        Ok(())
    }

    pub fn update_schema(&self, fields: &[FieldSchema]) -> Result<(), SchemaError> {
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
    /// Batch APIs ie above for multiple documents - todo

    /// Todo: move jni specific apis to jni module
    #[deprecated]
    pub fn jni_upsert_field_values(
        &self,
        primary_key: Vec<u8>,
        mut field_names: Vec<String>,
        mut field_values: Vec<Vec<u8>>,
    ) -> Result<(), IndexError> {
        let mut document: HashMap<String, FieldValue> =
            HashMap::with_capacity(field_names.len() + 1);

        let schema = self.schema.read().unwrap();

        // insert primary key
        let mut value = FieldValue::new();
        value.set_bytesValue(primary_key);
        document.insert(schema.primary_key_field_name().to_string(), value);

        // insert other fields
        for _ in 0..field_names.len() {
            let mut value = FieldValue::new();
            value.set_bytesValue(field_values.pop().unwrap());

            document.insert(field_names.pop().unwrap(), value);
        }

        self.upsert_field_values(&document)
    }

    pub fn upsert_field_values(
        &self,
        document: &HashMap<String, FieldValue>,
    ) -> Result<(), IndexError> {
        if document.len() == 0 {
            return Ok(());
        }

        // extract primary key
        let primary_key = self.extract_primary_key(document);
        if primary_key.is_none() {
            return Err(IndexError::IllegalArguments(
                "Cannot upsert with missing primary-key".to_string(),
            ));
        }
        let primary_key = &primary_key.unwrap();
        if primary_key.len() > u16::MAX as usize {
            return Err(IndexError::IllegalArguments(
                "primary_key larger than 64KB is unsupported".to_string(),
            ));
        }

        let schema = self.schema.read().unwrap();

        // flatten to vectors
        let mut fields = Vec::with_capacity(document.len());
        let mut values = Vec::with_capacity(document.len());
        for (field_name, field_value) in document.iter() {
            fields.push(
                schema
                    .fetch_field_by_name(field_name)
                    .expect("no new fields expected"), // TODO: there can be a mismatch b/w rust and proto enums
            );
            values.push(
                TryInto::<IndexedValue>::try_into(field_value)
                    .expect("no new field types expected"),
            );
        }

        let index_id = fxhash::hash(primary_key) % NUM_SEGMENTS;
        let mut ckv_index_segment = self.segments[index_id].write().unwrap();
        ckv_index_segment.upsert_document(primary_key, fields, values)?;
        Ok(())
    }

    #[deprecated]
    pub fn legacy_delete_field_values(
        &self,
        primary_key: &[u8],
        field_names: Vec<String>,
    ) -> io::Result<()> {
        if primary_key.len() == 0 {
            return Ok(());
        }

        let schema = self.schema.read().unwrap();
        let mut final_fields = Vec::with_capacity(field_names.len());
        for i in 0..field_names.len() {
            if let Some(field) = schema.fetch_field_by_name(&field_names[i]) {
                final_fields.push(field);
            }
        }

        if final_fields.len() == 0 {
            return Ok(());
        }

        let index_id = fxhash::hash(primary_key) % NUM_SEGMENTS;
        let mut ckv_index_segment = self.segments[index_id].write().unwrap();
        ckv_index_segment.delete_field_values(primary_key, final_fields)
    }

    pub fn delete_field_values(
        &self,
        document: &HashMap<String, FieldValue>,
        field_names: &[String],
    ) -> Result<(), IndexError> {
        if document.len() == 0 || field_names.len() == 0 {
            return Ok(());
        }

        // extract primary key
        let primary_key = self.extract_primary_key(document);
        if primary_key.is_none() {
            return Err(IndexError::IllegalArguments(
                "Cannot delete with missing primary-key".to_string(),
            ));
        }
        let primary_key = &primary_key.unwrap();

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

        let index_id = fxhash::hash(primary_key) % NUM_SEGMENTS;
        let mut ckv_index_segment = self.segments[index_id].write().unwrap();
        ckv_index_segment.delete_field_values(primary_key, fields)?;

        Ok(())
    }

    #[deprecated]
    pub fn legacy_delete_document(&self, primary_key: &[u8]) -> io::Result<()> {
        if primary_key.len() == 0 {
            return Ok(());
        }

        let index_id = fxhash::hash(primary_key) % NUM_SEGMENTS;
        let mut ckv_index_segment = self.segments[index_id].write().unwrap();
        ckv_index_segment.delete_document(primary_key)
    }

    /// Delete a document, given its primary key.
    pub fn delete_document(
        &self,
        document: &HashMap<String, FieldValue>,
    ) -> Result<(), IndexError> {
        if document.len() == 0 {
            return Ok(());
        }

        // extract primary key
        let primary_key = self.extract_primary_key(document);
        if primary_key.is_none() {
            return Err(IndexError::IllegalArguments(
                "Cannot delete with missing primary-key".to_string(),
            ));
        }
        let primary_key = &primary_key.unwrap();

        let index_id = fxhash::hash(primary_key) % NUM_SEGMENTS;
        let mut ckv_index_segment = self.segments[index_id].write().unwrap();
        ckv_index_segment.delete_document(primary_key)?;

        Ok(())
    }

    fn extract_primary_key(&self, document: &HashMap<String, FieldValue>) -> Option<Vec<u8>> {
        if document.len() == 0 {
            return None;
        }

        let schema = self.schema.read().unwrap();

        // extract primary key
        let primary_key = schema.extract_primary_key(&document)?;
        let primary_key: IndexedValue =
            primary_key.try_into().expect("no new field types expected"); // TODO: inspect!!

        Some(primary_key.serialize())
    }
}
