package io.inline.clients;

import com.google.common.collect.ImmutableMap;
import com.inlineio.schemas.Common;
import com.inlineio.schemas.Services;
import com.inlineio.schemas.Streaming;
import io.inline.clients.internal.IKVClientJNI;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class TestingInlineKVReader implements InlineKVReader, InlineKVWriter {
    private final DefaultInlineKVReader _defaultInlineKVReader;
    private final ClientOptions _options;

    public static final Map<String, Integer> FIELD_NAME_TO_FIELD_ID_MAPPING = new ImmutableMap.Builder<String, Integer>()
            .put("key", 0)  // primary-key, bytes field
            .put("name", 1)  // string field
            .put("profile", 2)  // bytes field
            .build();

    public TestingInlineKVReader(ClientOptions options) {
        _defaultInlineKVReader = new DefaultInlineKVReader();
        _options = options;
    }

    @Override
    public void startup(ClientOptions noop) throws RuntimeException {
        // Reader Startup
        _defaultInlineKVReader.startup(_options);
    }

    @Override
    public void startup() {
        // Writer startup
        // Call reader once again
        startup(_options);
    }

    @Override
    public void shutdown() throws RuntimeException {
        _defaultInlineKVReader.shutdown();
    }

    @Override
    public void upsertFieldValues(IKVDocument document) {
        Map<String, Services.FieldValue> fieldValues = document.asMap();
        Services.MultiFieldDocument multiFieldDocument = Services.MultiFieldDocument.newBuilder()
                .putAllDocument(fieldValues)
                .build();
        List<Common.FieldSchema> schema = extractSchema(fieldValues);

        Streaming.IKVDataEvent event = Streaming.IKVDataEvent.newBuilder()
                .addAllFieldSchema(schema)
                .setUpsertDocumentFieldsEvent(Streaming.UpsertDocumentFieldsEvent.newBuilder()
                        .setMultiFieldDocument(multiFieldDocument)
                        .build())
                .build();

        // jni call
        IKVClientJNI.processIKVDataEvent(_defaultInlineKVReader.handle(), event.toByteArray());
    }

    @Override
    public void deleteFieldValues(IKVDocument documentId, Collection<String> fieldsToDelete) {
        Map<String, Services.FieldValue> fieldValues = documentId.asMap();
        Services.MultiFieldDocument multiFieldDocument = Services.MultiFieldDocument.newBuilder()
                .putAllDocument(fieldValues)
                .build();
        List<Common.FieldSchema> schema = extractSchema(fieldValues);

        Streaming.IKVDataEvent event = Streaming.IKVDataEvent.newBuilder()
                .addAllFieldSchema(schema)
                .setDeleteDocumentFieldsEvent(Streaming.DeleteDocumentFieldsEvent.newBuilder()
                        .setDocumentId(multiFieldDocument)
                        .addAllFieldsToDelete(fieldsToDelete)
                        .build())
                .build();

        // jni call
        IKVClientJNI.processIKVDataEvent(_defaultInlineKVReader.handle(), event.toByteArray());
    }


    @Override
    public void deleteDocument(IKVDocument documentId) {
        Map<String, Services.FieldValue> fieldValues = documentId.asMap();
        Services.MultiFieldDocument multiFieldDocument = Services.MultiFieldDocument.newBuilder()
                .putAllDocument(fieldValues)
                .build();
        List<Common.FieldSchema> schema = extractSchema(fieldValues);

        Streaming.IKVDataEvent event = Streaming.IKVDataEvent.newBuilder()
                .addAllFieldSchema(schema)
                .setDeleteDocumentEvent(Streaming.DeleteDocumentEvent.newBuilder()
                        .setDocumentId(multiFieldDocument)
                        .build())
                .build();

        // jni call
        IKVClientJNI.processIKVDataEvent(_defaultInlineKVReader.handle(), event.toByteArray());
    }

    private static List<Common.FieldSchema> extractSchema(Map<String, Services.FieldValue> fieldValues) {
        List<Common.FieldSchema> schema = new ArrayList<>(fieldValues.size());
        for (Map.Entry<String, Services.FieldValue> entry: fieldValues.entrySet()) {
            String name = entry.getKey();
            int id = FIELD_NAME_TO_FIELD_ID_MAPPING.get(name);

            Common.FieldType fieldType;
            if (entry.getValue().hasStringValue()) {
                fieldType = Common.FieldType.STRING;
            } else if (entry.getValue().hasBytesValue()) {
                fieldType = Common.FieldType.BYTES;
            } else {
                throw new UnsupportedOperationException("not expecting fixed width types");
            }

            Common.FieldSchema fieldSchema = Common.FieldSchema.newBuilder()
                    .setName(name)
                    .setId(id)
                    .setFieldType(fieldType)
                    .build();
            schema.add(fieldSchema);
        }

        return schema;
    }

    @Nullable
    @Override
    public byte[] getBytesValue(PrimaryKey key, FieldAccessor fieldAccessor) {
        return _defaultInlineKVReader.getBytesValue(key, fieldAccessor);
    }

    @Override
    public List<byte[]> multiGetBytesValue(List<PrimaryKey> keys, FieldAccessor fieldAccessor) {
        return _defaultInlineKVReader.multiGetBytesValue(keys, fieldAccessor);
    }

    @Nullable
    @Override
    public String getStringValue(PrimaryKey key, FieldAccessor fieldAccessor) {
        return _defaultInlineKVReader.getStringValue(key, fieldAccessor);
    }

    @Override
    public List<String> multiGetStringValue(List<PrimaryKey> keys, FieldAccessor fieldAccessor) {
        return _defaultInlineKVReader.multiGetStringValue(keys, fieldAccessor);
    }

    @Override
    public void batchUpsertFieldValues(Collection<IKVDocument> documents) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void batchDeleteFieldValues(Collection<IKVDocument> documentIds, Collection<String> fieldsToDelete) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void batchDeleteDocuments(Collection<IKVDocument> documentIds) {
        throw new UnsupportedOperationException();
    }
}
