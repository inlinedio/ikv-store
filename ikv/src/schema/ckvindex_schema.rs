use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
};

use anyhow::bail;
use protobuf::Message;

use crate::proto::generated_proto::{common::FieldValue, index::SavedCKVIndexSchema};

use super::field::FieldId;

pub struct CKVIndexSchema {
    mount_directory: String,

    primary_key_field_name: String,

    field_name_to_id: HashMap<String, FieldId>,
}

impl CKVIndexSchema {
    /// Create with no fields.
    /// Fields are added lazily with update() methods.
    fn new(mount_directory: &str, primary_key: String) -> Self {
        Self {
            mount_directory: mount_directory.to_string(),
            primary_key_field_name: primary_key,
            field_name_to_id: HashMap::new(),
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

        let mut field_name_to_id = HashMap::new();
        for (fieldname, fieldid) in saved_schema.field_ids.iter() {
            field_name_to_id.insert(fieldname.to_string(), *fieldid);
        }

        Ok(CKVIndexSchema {
            mount_directory: mount_directory.to_string(),
            primary_key_field_name,
            field_name_to_id,
        })
    }

    // checks if a valid index is loaded at the mount directory
    // Returns error with some details if empty or invalid, else ok.
    pub fn is_valid_index(mount_directory: &str) -> anyhow::Result<()> {
        let filepath = format!("{}/schema", mount_directory);
        if !Path::new(&filepath).exists() {
            bail!("schema index not present");
        }
        Ok(())
    }

    pub fn delete_all(mount_directory: &str) -> anyhow::Result<()> {
        let filepath = format!("{}/schema", mount_directory);
        if Path::new(&filepath).exists() {
            std::fs::remove_dir_all(&filepath)?;
        }
        Ok(())
    }

    pub fn fetch_id_by_name(&self, field_name: &str) -> Option<FieldId> {
        self.field_name_to_id.get(field_name).copied()
    }

    pub fn extract_primary_key_value<'a>(
        &self,
        document: &'a HashMap<String, FieldValue>,
    ) -> Option<&'a FieldValue> {
        document.get(&self.primary_key_field_name)
    }

    /// Update the internal fields table with new field-info if required.
    /// Known fields are skipped, new start getting tracked.
    ///
    /// TODO - This operation can fail partially - ie schema for only some fields gets updated.
    pub fn upsert_schema(&mut self, document: &HashMap<String, FieldValue>) -> anyhow::Result<()> {
        let mut updated = false;

        let table = &mut self.field_name_to_id;
        for field_name in document.keys() {
            if !table.contains_key(field_name) {
                let field_id = table.len();
                table.insert(field_name.clone(), field_id as FieldId);

                updated = true; /* needs disk update */
            }
        }

        if updated {
            self.save(&self.mount_directory)?;
        }

        Ok(())
    }

    /// TODO: how to handle failures (ex. persisting to disk) gracefully??
    fn save(&self, mount_directory: &str) -> std::io::Result<()> {
        // serialize with proto
        let mut field_ids = HashMap::new();
        for (fieldname, fieldid) in self.field_name_to_id.iter() {
            field_ids.insert(fieldname.clone(), *fieldid);
        }

        let mut saved_schema = SavedCKVIndexSchema::new();
        saved_schema.primary_key_field_name = self.primary_key_field_name.clone();
        saved_schema.field_ids = field_ids;

        let contents = saved_schema.write_to_bytes()?;

        // truncate existing schema file, write new version
        let filepath = format!("{}/schema", mount_directory);
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(filepath)?;
        let mut writer = BufWriter::new(file);
        writer.write_all(&contents)?;
        Ok(())
    }
}
