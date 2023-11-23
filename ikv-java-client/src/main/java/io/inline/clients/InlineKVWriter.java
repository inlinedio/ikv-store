package io.inline.clients;
import com.inlineio.schemas.Services.FieldValue;

import java.util.Collection;
import java.util.Map;

public interface InlineKVWriter {
    void startup();
    void shutdown();

    void upsertFieldValues(IKVDocument document);
    void batchUpsertFieldValues(Collection<IKVDocument> documents);

    void deleteFieldValues(IKVDocument documentId, Collection<String> fieldsToDelete);
    void batchDeleteFieldValues(Collection<IKVDocument> documentIds, Collection<String> fieldsToDelete);

    void deleteDocument(IKVDocument documentId);
    void batchDeleteDocuments(Collection<IKVDocument> documentIds);
}