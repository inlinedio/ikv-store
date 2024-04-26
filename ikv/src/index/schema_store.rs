use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
};

use anyhow::bail;
use protobuf::Message;

use crate::proto::generated_proto::{common::FieldValue, index::SavedCKVIndexSchema};
use crate::schema::field::FieldId;

#[cfg(test)]
#[path = "schema_store_test.rs"]
mod schema_store_test;

#[derive(Debug)]
pub struct CKVIndexSchema {
    mount_directory: String,

    primary_key_field_name: String,

    field_name_to_id: HashMap<String, FieldId>,

    // Used to assign field-ids to new fields. Primary-key always gets id=0.
    // Current value denotes an available id, i.e. [0, current-1] are already taken.
    // It's value always increases, and can only go down if compacted.
    field_id_counter: u64,
}

impl CKVIndexSchema {
    /// Create with no fields.
    /// Fields are added lazily with update() methods.
    fn new(mount_directory: &str, primary_key: String) -> anyhow::Result<Self> {
        let mut field_name_to_id = HashMap::new();
        field_name_to_id.insert(primary_key.clone(), 0 as FieldId);

        let index = Self {
            mount_directory: mount_directory.to_string(),
            primary_key_field_name: primary_key,
            field_name_to_id,
            field_id_counter: 1,
        };

        index.save()?;
        Ok(index)
    }

    pub fn open_or_create(mount_directory: &str, primary_key: String) -> anyhow::Result<Self> {
        let file_path = format!("{}/schema", mount_directory);
        if !Path::new(&file_path).exists() {
            // no schema file, assume new store for host
            return CKVIndexSchema::new(mount_directory, primary_key);
        }

        let file = OpenOptions::new().read(true).open(file_path)?;
        let mut reader = BufReader::new(file);

        let mut contents: Vec<u8> = vec![];
        reader.read_to_end(&mut contents)?;
        if contents.len() == 0 {
            // empty schema file, unreachable
            // TODO: add this to is_valid_index check
            bail!("Index schema store has no content and no primary key, invalid state");
        }

        let saved_schema = SavedCKVIndexSchema::parse_from_bytes(&contents)?;

        let mut field_id_counter = saved_schema.field_ids.len() as u64;
        if saved_schema.field_id_counter != 0 {
            field_id_counter = saved_schema.field_id_counter;
        }

        Ok(CKVIndexSchema {
            mount_directory: mount_directory.to_string(),
            primary_key_field_name: saved_schema.primary_key_field_name,
            field_name_to_id: saved_schema.field_ids,
            field_id_counter,
        })
    }

    #[cfg(test)]
    pub fn close(self) {
        // no op
    }

    pub fn index_not_present(mount_directory: &str) -> bool {
        let filepath = format!("{}/schema", mount_directory);
        !Path::new(&filepath).exists()
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
            std::fs::remove_file(&filepath)?;
        }
        Ok(())
    }

    pub fn compact(&mut self) -> anyhow::Result<Vec<FieldId>> {
        // calculate new-field-id -> old-field-id mapping
        let mut new_fid_to_old_fid: Vec<FieldId> = Vec::with_capacity(self.field_name_to_id.len());
        let mut old_fid_to_field_name: HashMap<FieldId, String> =
            HashMap::with_capacity(self.field_name_to_id.len());
        for (field_name, field_id) in self.field_name_to_id.iter() {
            new_fid_to_old_fid.push(*field_id as FieldId);
            old_fid_to_field_name.insert(*field_id as FieldId, field_name.clone());
        }
        new_fid_to_old_fid.sort();

        // change string->id mappings
        let mut field_name_to_id = HashMap::with_capacity(new_fid_to_old_fid.len());
        for new_fid in 0..new_fid_to_old_fid.len() {
            let old_fid = new_fid_to_old_fid[new_fid];
            let field_name = old_fid_to_field_name.get(&old_fid).unwrap();
            field_name_to_id.insert(field_name.clone(), new_fid as FieldId);
        }

        // reset counter and save to disk
        self.field_name_to_id = field_name_to_id;
        self.field_id_counter = self.field_name_to_id.len() as u64;
        self.save()?;

        Ok(new_fid_to_old_fid)
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
                let field_id = self.field_id_counter;
                self.field_id_counter += 1;
                table.insert(field_name.clone(), field_id as FieldId);

                updated = true; /* needs disk update */
            }
        }

        if updated {
            self.save()?;
        }

        Ok(())
    }

    /// Drops provided fields (exact names or prefixes). Ignores attempt to drop primary-key.
    /// We do NOT reset field_id counter, since new fields need bigger ids
    pub fn soft_delete_fields(
        &mut self,
        field_names: &[String],
        field_name_prefixes: &[String],
    ) -> anyhow::Result<()> {
        // exact field names to delete
        let mut fields_to_delete = field_names.to_vec();

        // prefix search
        for prefix in field_name_prefixes.iter() {
            for fieldname in self.field_name_to_id.keys() {
                if fieldname.starts_with(prefix) {
                    fields_to_delete.push(fieldname.clone());
                }
            }
        }

        if fields_to_delete.is_empty() {
            return Ok(());
        }

        let mut updated = false;
        for fieldname in fields_to_delete.iter() {
            if fieldname != &self.primary_key_field_name {
                if self.field_name_to_id.remove(fieldname).is_some() {
                    updated = true;
                }
            }
        }

        if updated {
            self.save()?;
        }

        Ok(())
    }

    /// Drops all fields except primary-key.
    /// We also reset field_id counter - since this is used for hard data delete.
    pub fn hard_delete_all_fields(&mut self) -> anyhow::Result<()> {
        self.field_id_counter = 1;

        // modify map
        let mut field_name_to_id = HashMap::new();
        field_name_to_id.insert(self.primary_key_field_name.clone(), 0 as FieldId);
        self.field_name_to_id = field_name_to_id;
        self.save()?;

        Ok(())
    }

    /// TODO: how to handle failures (ex. persisting to disk) gracefully??
    fn save(&self) -> std::io::Result<()> {
        // serialize with proto
        let mut field_ids = HashMap::new();
        for (fieldname, fieldid) in self.field_name_to_id.iter() {
            field_ids.insert(fieldname.clone(), *fieldid);
        }

        let mut saved_schema = SavedCKVIndexSchema::new();
        saved_schema.primary_key_field_name = self.primary_key_field_name.clone();
        saved_schema.field_ids = field_ids;
        saved_schema.field_id_counter = self.field_id_counter;

        let contents = saved_schema.write_to_bytes()?;

        // truncate existing schema file, write new version
        let filepath = format!("{}/schema", self.mount_directory);
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
