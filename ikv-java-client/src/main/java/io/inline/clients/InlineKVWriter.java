package io.inline.clients;
import com.inlineio.schemas.Services.FieldValue;

import java.util.Collection;
import java.util.Map;

public interface InlineKVWriter {
    void startup();
    void shutdown();

    void upsertFieldValues(Map<String, FieldValue> docFieldValues);
    void batchUpsertFieldValues(Collection<Map<String, FieldValue>> docFieldValuesCollection);

    void deleteFieldValues(Map<String, FieldValue> documentId, Collection<String> fieldsToDelete);
    void batchDeleteFieldValues(Collection<Map<String, FieldValue>> documentIds, Collection<String> fieldsToDelete);

    void deleteDocument(Map<String, FieldValue> documentId);
    void batchDeleteDocuments(Collection<Map<String, FieldValue>> documentIds);
}
