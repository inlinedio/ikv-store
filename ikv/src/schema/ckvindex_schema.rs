use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
};

use protobuf::Message;

use crate::proto::generated_proto::{
    common::FieldSchema,
    internal::{SavedCKVIndexSchema, SavedField},
    services::FieldValue,
};

use super::{error::SchemaError, field::Field};

pub struct CKVIndexSchema {
    primary_key_field_name: String,

    // field-name -> Field
    fieldname_field_table: HashMap<String, Field>,
}

impl CKVIndexSchema {
    /// Create with no fields.
    /// Fields are added lazily with update() methods.
    fn new(primary_key: String) -> Self {
        Self {
            // TODO: accept correct primary key
            primary_key_field_name: primary_key,
            fieldname_field_table: HashMap::new(),
        }
    }

    pub fn open_or_create(mount_directory: &str, primary_key: String) -> std::io::Result<Self> {
        let file_path = format!("{}/schema", mount_directory);
        if !Path::new(&file_path).exists() {
            // no schema file, assume new store for host
            return Ok(CKVIndexSchema::new(primary_key));
        }

        let file = OpenOptions::new().read(true).open(file_path)?;
        let mut reader = BufReader::new(file);

        let mut contents: Vec<u8> = vec![];
        reader.read_to_end(&mut contents)?;
        if contents.len() == 0 {
            // empty schema file, assume new store for host
            return Ok(CKVIndexSchema::new(primary_key));
        }

        let saved_schema = SavedCKVIndexSchema::parse_from_bytes(&contents)?;
        let primary_key_field_name = saved_schema.primary_key_field_name;

        let mut fieldname_field_table = HashMap::new();
        for (fieldname, savedfield) in saved_schema.fields.iter() {
            let field = Field::new(
                savedfield.name.clone(),
                savedfield.id as u16,
                savedfield.field_type.enum_value().unwrap(),
            );
            fieldname_field_table.insert(fieldname.to_string(), field);
        }

        Ok(CKVIndexSchema {
            primary_key_field_name,
            fieldname_field_table,
        })
    }

    // TODO: we need continuous serialization - when the process getting killed abruptly.
    // simple sol - do it when we see a new field??
    pub fn save(&self, mount_directory: &str) -> std::io::Result<()> {
        let file_path = format!("{}/schema", mount_directory);
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(file_path)?;
        let mut writer = BufWriter::new(file);

        // serialize with proto
        let mut saved_fields = HashMap::new();
        for (fieldname, field) in self.fieldname_field_table.iter() {
            let mut saved_field = SavedField::new();
            saved_field.name = field.name().to_string();
            saved_field.id = field.id() as i32;
            saved_field.field_type = field.field_type().clone().into();
            saved_fields.insert(fieldname.clone(), saved_field);
        }

        let mut saved_schema = SavedCKVIndexSchema::new();
        saved_schema.primary_key_field_name = self.primary_key_field_name.clone();
        saved_schema.fields = saved_fields;

        let contents = saved_schema.write_to_bytes()?;
        writer.write_all(&contents)?;

        Ok(())
    }

    pub fn fetch_field_by_name<'a>(&'a self, field_name: &str) -> Option<&'a Field> {
        self.fieldname_field_table.get(field_name)
    }

    pub fn extract_primary_key<'a>(
        &self,
        document: &'a HashMap<String, FieldValue>,
    ) -> Option<&'a FieldValue> {
        document.get(&self.primary_key_field_name)
    }

    /// Update the internal fields table with new field-info if required.
    /// Known fields are skipped, new start getting tracked.
    ///
    /// TODO: new fields need to be persisted to schema on disk.
    pub fn update(&mut self, fields: &[FieldSchema]) -> Result<(), SchemaError> {
        let table = &mut self.fieldname_field_table;
        for field_schema in fields {
            if !table.contains_key(&field_schema.name) {
                let field = field_schema.try_into()?;
                table.insert(field_schema.name.to_string(), field);
            }
        }
        Ok(())
    }
}
