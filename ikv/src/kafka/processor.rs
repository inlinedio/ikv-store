use std::sync::Arc;

use crate::index::ckv::CKVIndex;
use crate::proto::generated_proto::services::field_value;
use crate::proto::generated_proto::streaming::{
    DeleteDocumentEvent, DeleteDocumentFieldsEvent, IKVDataEvent, UpsertDocumentFieldsEvent,
};
use crate::schema::field::FieldValue;

use super::error::IKVKafkaError;

pub struct WritesProcessor {
    ckv_index: Arc<CKVIndex>,
}

impl WritesProcessor {
    pub fn new(ckv_index: Arc<CKVIndex>) -> Self {
        Self { ckv_index }
    }

    pub fn process(&self, event: &IKVDataEvent) -> Result<(), IKVKafkaError> {
        // update schema (?new fields?)
        let field_schema = event.fieldSchema.as_slice();
        self.ckv_index.update_schema(field_schema)?;

        if let Some(inner_event) = event.event.as_ref() {
            match inner_event {
                crate::proto::generated_proto::streaming::ikvdata_event::Event::UpsertDocumentFieldsEvent(e) => return self.process_upsert(e),
                crate::proto::generated_proto::streaming::ikvdata_event::Event::DeleteDocumentFieldsEvent(e) => return self.process_field_delete(e),
                crate::proto::generated_proto::streaming::ikvdata_event::Event::DeleteDocumentEvent(e) => return self.process_document_delete(e),
            };
        }

        Ok(())
    }

    fn process_upsert(&self, event: &UpsertDocumentFieldsEvent) -> Result<(), IKVKafkaError> {
        if event.multiFieldDocument.is_none() {
            return Ok(());
        }

        let mfd = event.multiFieldDocument.as_ref().unwrap();
        for (field_name, field_value) in mfd.document.iter() {
            let field_value: FieldValue = field_value.try_into()?;
        }

        todo!()
    }

    fn process_field_delete(&self, event: &DeleteDocumentFieldsEvent) -> Result<(), IKVKafkaError> {
        todo!()
    }

    fn process_document_delete(&self, event: &DeleteDocumentEvent) -> Result<(), IKVKafkaError> {
        todo!()
    }
}
