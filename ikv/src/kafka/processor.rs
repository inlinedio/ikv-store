use std::sync::Arc;

use anyhow::Ok;

use crate::index::ckv::CKVIndex;
use crate::proto::generated_proto::streaming::ikvdata_event::Event;
use crate::proto::generated_proto::streaming::{
    DeleteDocumentEvent, DeleteDocumentFieldsEvent, DropFieldEvent, IKVDataEvent,
    UpsertDocumentFieldsEvent,
};

pub struct WritesProcessor {
    ckv_index: Arc<CKVIndex>,
}

impl WritesProcessor {
    pub fn new(ckv_index: Arc<CKVIndex>) -> Self {
        Self { ckv_index }
    }

    /// Flush all writes to disk.
    /// Should be called before checkpointing of
    /// incoming message stream.
    pub fn flush_all(&self) -> anyhow::Result<()> {
        self.ckv_index.flush_writes()
    }

    pub fn process(&self, event: &IKVDataEvent) -> anyhow::Result<()> {
        // dispatch to inner event processors
        if let Some(inner_event) = event.event.as_ref() {
            match inner_event {
                Event::UpsertDocumentFieldsEvent(e) => return self.process_upsert(e),
                Event::DeleteDocumentFieldsEvent(e) => return self.process_field_delete(e),
                Event::DeleteDocumentEvent(e) => return self.process_document_delete(e),
                Event::DropFieldEvent(e) => return self.process_drop_fields(e),
            };
        }

        Ok(())
    }

    fn process_upsert(&self, event: &UpsertDocumentFieldsEvent) -> anyhow::Result<()> {
        if event.document.is_none() {
            return Ok(());
        }

        let document_on_wire = event.document.as_ref().unwrap();
        self.ckv_index
            .upsert_field_values(&document_on_wire.document)
    }

    fn process_field_delete(&self, event: &DeleteDocumentFieldsEvent) -> anyhow::Result<()> {
        if event.documentId.is_none() {
            return Ok(());
        }

        let document_on_wire = event.documentId.as_ref().unwrap();
        let field_names = &event.fieldsToDelete;
        if field_names.len() == 0 {
            return Ok(());
        }

        self.ckv_index
            .delete_field_values(&document_on_wire.document, field_names)
    }

    fn process_document_delete(&self, event: &DeleteDocumentEvent) -> anyhow::Result<()> {
        if event.documentId.is_none() {
            return Ok(());
        }

        let document_on_wire = event.documentId.as_ref().unwrap();
        self.ckv_index.delete_document(&document_on_wire.document)
    }

    fn process_drop_fields(&self, event: &DropFieldEvent) -> anyhow::Result<()> {
        if !event.field_names.is_empty() || !event.field_name_prefixes.is_empty() {
            return self
                .ckv_index
                .drop_fields(&event.field_names, &event.field_name_prefixes);
        } else if event.drop_all {
            return self.ckv_index.drop_all_documents();
        }

        Ok(())
    }
}
