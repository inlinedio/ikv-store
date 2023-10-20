use yaml_rust::Yaml;

use crate::schema::field::Field;

use super::primary_key_index::PrimaryKeyIndex;
use std::{
    io::{self, Error},
    sync::RwLock,
};

const NUM_SEGMENTS: usize = 16;

pub struct ColumnarKVIndex {
    // hash(key) -> PrimaryKeyIndex
    segments: Vec<RwLock<PrimaryKeyIndex>>,

    // field-id -> Field
    fieldid_field_table: Vec<Field>,
}

impl ColumnarKVIndex {
    pub fn new(mount_directory: String, schema: &Yaml) -> io::Result<ColumnarKVIndex> {
        let mut segments = vec![];
        for index_id in 0..NUM_SEGMENTS {
            let segment = PrimaryKeyIndex::new(&mount_directory, index_id)?;
            segments.push(RwLock::new(segment));
        }

        Ok(Self {
            segments,
            fieldid_field_table: crate::schema::load_yaml_schema(schema),
        })
    }

    pub fn open(mount_directory: String) -> io::Result<ColumnarKVIndex> {
        // read schema file
        // TODO!
        let fields = vec![];

        let mut segments = vec![];
        for index_id in 0..NUM_SEGMENTS {
            let segment = PrimaryKeyIndex::open(&mount_directory, index_id)?;
            segments.push(RwLock::new(segment));
        }

        Ok(Self {
            segments,
            fieldid_field_table: fields,
        })
    }

    pub fn read_field_by_id(&self, document_id: &[u8], field_id: u16) -> Option<Vec<u8>> {
        let field = self.fieldid_field_table.get(field_id as usize)?;
        let segment_id = fxhash::hash(document_id) % NUM_SEGMENTS;
        let segment = self.segments[segment_id].read().unwrap();
        segment.read(&document_id, field)
    }

    pub fn upsert_field_by_id(
        &self,
        document_id: &[u8],
        field_value: &[u8],
        field_id: u16,
    ) -> io::Result<()> {
        let field = self.fieldid_field_table.get(field_id as usize);
        if field.is_none() {
            return Err(Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid field_id",
            ));
        }
        let field = field.unwrap();

        let segment_id = fxhash::hash(document_id) % NUM_SEGMENTS;
        let mut segment = self.segments[segment_id].write().unwrap();
        segment.upsert(document_id, field, field_value)
    }
}
