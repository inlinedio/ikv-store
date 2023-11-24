use std::collections::HashMap;

use crate::proto::generated_proto::{common::FieldSchema, services::FieldValue};

use super::{error::SchemaError, field::Field};

pub struct CKVIndexSchema {
    primary_key_field_name: String,

    // field-name -> Field
    fieldname_field_table: HashMap<String, Field>,
}

impl CKVIndexSchema {
    /// Create with no fields.
    /// Fields are added lazily with update() methods.
    pub fn new(initial_fiels: HashMap<String, Field>) -> Self {
        Self {
            // TODO: accept correct primary key
            primary_key_field_name: "".to_string(),
            fieldname_field_table: initial_fiels,
        }
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

    pub fn primary_key_field_name(&self) -> &str {
        &self.primary_key_field_name
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
