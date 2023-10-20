use crate::schema::{self, field::Field};

use super::primary_key_index::PrimaryKeyIndex;
use std::{
    fs::OpenOptions,
    io::{self, Error, Read},
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
    pub fn new(mount_directory: String, schema: &str) -> io::Result<ColumnarKVIndex> {
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
        let mut schema_file = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(format!("{}/schema", mount_directory))?;
        let mut schema_str = String::new();
        schema_file.read_to_string(&mut schema_str)?;
        let fields = schema::load_yaml_schema(&schema_str);

        // open index segments
        let mut segments = Vec::with_capacity(NUM_SEGMENTS);
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
        let index_id: usize = fxhash::hash(document_id) % NUM_SEGMENTS;
        let primary_key_index: std::sync::RwLockReadGuard<'_, PrimaryKeyIndex> =
            self.segments[index_id].read().unwrap();
        primary_key_index.read(&document_id, field)
    }

    pub fn upsert_field_by_id(
        &self,
        document_id: &[u8],
        field_value: &[u8],
        field_id: u16,
    ) -> io::Result<()> {
        if document_id.len() == 0 {
            return Err(Error::new(
                std::io::ErrorKind::InvalidInput,
                "empty document_id not allowed",
            ));
        }
        if document_id.len() > u16::MAX as usize {
            return Err(Error::new(
                std::io::ErrorKind::Unsupported,
                "document_id larger than 64KB is unsupported",
            ));
        }

        if field_value.len() == 0 {
            return Err(Error::new(
                std::io::ErrorKind::InvalidInput,
                "empty field_value not allowed",
            ));
        }
        if field_value.len() > u32::MAX as usize {
            return Err(Error::new(
                std::io::ErrorKind::Unsupported,
                "value larger than 4GB is unsupported",
            ));
        }

        let field = self.fieldid_field_table.get(field_id as usize);
        if field.is_none() {
            return Err(Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid field_id",
            ));
        }

        let field = field.unwrap();
        let index_id = fxhash::hash(document_id) % NUM_SEGMENTS;
        let mut primary_key_index = self.segments[index_id].write().unwrap();
        primary_key_index.upsert(document_id, field_value, field)
    }
}
