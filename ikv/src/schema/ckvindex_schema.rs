use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
};

use protobuf::Message;

use crate::proto::generated_proto::{
    common::FieldSchema, common::FieldValue, index::SavedCKVIndexSchema,
};

use super::field::Field;

pub struct CKVIndexSchema {
    mount_directory: String,

    primary_key_field_name: String,

    // field-name -> Field
    fieldname_field_table: HashMap<String, Field>,
}

impl CKVIndexSchema {
    /// Create with no fields.
    /// Fields are added lazily with update() methods.
    fn new(mount_directory: &str, primary_key: String) -> Self {
        Self {
            mount_directory: mount_directory.to_string(),
            primary_key_field_name: primary_key,
            fieldname_field_table: HashMap::new(),
        }
    }

    pub fn open_or_create(mount_directory: &str, primary_key: String) -> anyhow::Result<Self> {
        let file_path = format!("{}/schema", mount_directory);
        if !Path::new(&file_path).exists() {
            // no schema file, assume new store for host
            return Ok(CKVIndexSchema::new(mount_directory, primary_key));
        }

        let file = OpenOptions::new().read(true).open(file_path)?;
        let mut reader = BufReader::new(file);

        let mut contents: Vec<u8> = vec![];
        reader.read_to_end(&mut contents)?;
        if contents.len() == 0 {
            // empty schema file, assume new store for host
            return Ok(CKVIndexSchema::new(mount_directory, primary_key));
        }

        let saved_schema = SavedCKVIndexSchema::parse_from_bytes(&contents)?;
        let primary_key_field_name = saved_schema.primary_key_field_name;

        let mut fieldname_field_table = HashMap::new();
        for (fieldname, fieldschema) in saved_schema.fields.iter() {
            let field = fieldschema.try_into()?;
            fieldname_field_table.insert(fieldname.to_string(), field);
        }

        Ok(CKVIndexSchema {
            mount_directory: mount_directory.to_string(),
            primary_key_field_name,
            fieldname_field_table,
        })
    }

    pub fn save(&self, mount_directory: &str) -> std::io::Result<()> {
        // serialize with proto
        let mut fields = HashMap::new();
        for (fieldname, field) in self.fieldname_field_table.iter() {
            let mut field_schema = FieldSchema::new();
            field_schema.name = field.name().to_string();
            field_schema.id = field.id() as i32;
            field_schema.fieldType = field.field_type().clone().into();
            fields.insert(fieldname.clone(), field_schema);
        }

        let mut saved_schema = SavedCKVIndexSchema::new();
        saved_schema.primary_key_field_name = self.primary_key_field_name.clone();
        saved_schema.fields = fields;

        let contents = saved_schema.write_to_bytes()?;

        // truncate existing schema file, write new version
        let file_path = format!("{}/schema", mount_directory);
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(file_path)?;
        let mut writer = BufWriter::new(file);
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
    /// This operation can fail partially - ie schema for only some fields gets updated.
    pub fn update(&mut self, fields: &[FieldSchema]) -> anyhow::Result<()> {
        let mut conversion_error: Option<anyhow::Error> = None;
        let mut updated = false;

        let table = &mut self.fieldname_field_table;
        for field_schema in fields {
            if !table.contains_key(&field_schema.name) {
                match field_schema.try_into() {
                    Ok(field) => {
                        table.insert(field_schema.name.to_string(), field);
                        updated = true;
                    }
                    Err(e) => conversion_error = Some(e),
                }
            }
        }

        if updated {
            self.save(&self.mount_directory)?;
        }

        if let Some(e) = conversion_error {
            return Err(e);
        }

        Ok(())
    }
}
