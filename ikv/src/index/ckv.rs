use crate::{
    index::ckv_segment,
    schema::{self, field::Field},
};

use super::ckv_segment::CKVIndexSegment;
use std::{
    collections::HashMap,
    fs::{self, OpenOptions},
    io::{self, BufReader, BufWriter, Error, Read, Write},
    sync::{RwLock, RwLockReadGuard},
};

const NUM_SEGMENTS: usize = 16;

/// Memmap based columnar key-value index.
pub struct CKVIndex {
    // hash(key) -> PrimaryKeyIndex
    segments: Vec<RwLock<CKVIndexSegment>>,

    // field-name -> Field
    fieldname_field_table: HashMap<String, Field>,
}

impl CKVIndex {
    pub fn new(mount_directory: &str, schema_file_path: &str) -> io::Result<CKVIndex> {
        // ensure mount_directory exists
        fs::create_dir_all(mount_directory.clone())?;

        // copy and persist schema file
        let mount_schema_file_path = format!("{}/schema", mount_directory);
        fs::copy(schema_file_path, format!("{}/schema", mount_directory))?;

        let schema_string = schema::read_schema_file(&mount_schema_file_path)?;

        let mut segments = vec![];
        for index_id in 0..NUM_SEGMENTS {
            let segment = CKVIndexSegment::new(mount_directory, index_id)?;
            segments.push(RwLock::new(segment));
        }

        let mut fieldid_field_table = schema::load_yaml_schema(&schema_string);
        schema::sort_by_field_id(&mut fieldid_field_table);
        let fieldname_field_table = schema::to_map(&fieldid_field_table);

        Ok(Self {
            segments,
            fieldname_field_table,
        })
    }

    pub fn open(mount_directory: String) -> io::Result<CKVIndex> {
        // read persisted schema into String
        let schema_file_path = format!("{}/schema", mount_directory);
        let schema_string = schema::read_schema_file(&schema_file_path)?;

        let mut fieldid_field_table = schema::load_yaml_schema(&schema_string);
        schema::sort_by_field_id(&mut fieldid_field_table);
        let fieldname_field_table = schema::to_map(&fieldid_field_table);

        // open index segments
        let mut segments = Vec::with_capacity(NUM_SEGMENTS);
        for index_id in 0..NUM_SEGMENTS {
            let segment = CKVIndexSegment::open(&mount_directory, index_id)?;
            segments.push(RwLock::new(segment));
        }

        Ok(Self {
            segments,
            fieldname_field_table,
        })
    }

    pub fn close(&self) {
        for segment in self.segments.iter() {
            segment.read().unwrap().close();
        }
    }

    /// Fetch field value for a primary key.
    pub fn get_field_value(&self, primary_key: &[u8], field_name: &str) -> Option<Vec<u8>> {
        let field = self.fieldname_field_table.get(field_name)?;

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
        let mut fields: Vec<&Field> = Vec::with_capacity(field_names.len());
        for field_name in field_names {
            let field = self
                .fieldname_field_table
                .get(&field_name)
                .expect("cannot handle unknonw field_names");
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

    pub fn upsert_field_values(
        &self,
        primary_key: &[u8],
        field_names: Vec<String>,
        field_values: Vec<Vec<u8>>,
    ) -> io::Result<()> {
        if primary_key.len() == 0 {
            return Ok(());
        }

        if primary_key.len() > u16::MAX as usize {
            return Err(Error::new(
                std::io::ErrorKind::Unsupported,
                "primary_key larger than 64KB is unsupported",
            ));
        }

        if field_names.len() != field_values.len() {
            return Err(Error::new(
                std::io::ErrorKind::InvalidData,
                "field name and value lengths mismatch",
            ));
        }

        // filter out unknown fields
        let mut final_fields = Vec::with_capacity(field_names.len());
        let mut final_field_values = Vec::with_capacity(field_values.len());
        for i in 0..field_names.len() {
            if let Some(field) = self.fieldname_field_table.get(&field_names[i]) {
                final_fields.push(field);
                final_field_values.push(field_values[i].as_slice());
            }
        }

        if final_fields.len() == 0 {
            return Ok(());
        }

        let index_id = fxhash::hash(primary_key) % NUM_SEGMENTS;
        let mut ckv_index_segment = self.segments[index_id].write().unwrap();
        ckv_index_segment.upsert_field_values(primary_key, final_fields, final_field_values)
    }

    pub fn delete_field_values(
        &self,
        primary_key: &[u8],
        field_names: Vec<String>,
    ) -> io::Result<()> {
        if primary_key.len() == 0 {
            return Ok(());
        }

        let mut final_fields = Vec::with_capacity(field_names.len());
        for i in 0..field_names.len() {
            if let Some(field) = self.fieldname_field_table.get(&field_names[i]) {
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

    pub fn delete_document(&self, primary_key: &[u8]) -> io::Result<()> {
        if primary_key.len() == 0 {
            return Ok(());
        }

        let index_id = fxhash::hash(primary_key) % NUM_SEGMENTS;
        let mut ckv_index_segment = self.segments[index_id].write().unwrap();
        ckv_index_segment.delete_document(primary_key)
    }
}

#[cfg(test)]
mod tests {
    use super::CKVIndex;

    #[test]
    fn open() {
        let yaml_str = "
        document:
        - name: firstname
          id: 0
          type: string
        - name: age
          id: 1
          type: i32
        - name: profile
          id: 2
          type: bytes";
        let index = CKVIndex::new("/tmp/basic", yaml_str);
        assert!(index.is_ok());
    }
}
