use std::sync::Arc;

use crate::index::ckv::CKVIndex;
use crate::index::error::IndexError;
use crate::proto::generated_proto::streaming::ikvdata_event::Event;
use crate::proto::generated_proto::streaming::{
    DeleteDocumentEvent, DeleteDocumentFieldsEvent, IKVDataEvent, UpsertDocumentFieldsEvent,
};

pub struct WritesProcessor {
    ckv_index: Arc<CKVIndex>,
}

impl WritesProcessor {
    pub fn new(ckv_index: Arc<CKVIndex>) -> Self {
        Self { ckv_index }
    }

    pub fn process(&self, event: &IKVDataEvent) -> Result<(), IndexError> {
        // update schema (?new fields?)
        let field_schema = event.fieldSchema.as_slice();
        self.ckv_index.update_schema(field_schema)?;

        // handle UpsertDocumentFieldsEvent|DeleteDocumentFieldsEvent|DeleteDocumentEvent events
        if let Some(inner_event) = event.event.as_ref() {
            match inner_event {
                Event::UpsertDocumentFieldsEvent(e) => return self.process_upsert(e),
                Event::DeleteDocumentFieldsEvent(e) => return self.process_field_delete(e),
                Event::DeleteDocumentEvent(e) => return self.process_document_delete(e),
            };
        }

        Ok(())
    }

    fn process_upsert(&self, event: &UpsertDocumentFieldsEvent) -> Result<(), IndexError> {
        if event.multiFieldDocument.is_none() {
            return Ok(());
        }

        let multi_field_document = event.multiFieldDocument.as_ref().unwrap();
        self.ckv_index
            .upsert_field_values(&multi_field_document.document)
    }

    fn process_field_delete(&self, event: &DeleteDocumentFieldsEvent) -> Result<(), IndexError> {
        if event.documentId.is_none() {
            return Ok(());
        }

        let document_id = event.documentId.as_ref().unwrap();
        let field_names = &event.fieldsToDelete;
        if field_names.len() == 0 {
            return Ok(());
        }

        self.ckv_index
            .delete_field_values(&document_id.document, field_names)
    }

    fn process_document_delete(&self, event: &DeleteDocumentEvent) -> Result<(), IndexError> {
        if event.documentId.is_none() {
            return Ok(());
        }

        let document_id = event.documentId.as_ref().unwrap();
        self.ckv_index.delete_document(&document_id.document)
    }
}
