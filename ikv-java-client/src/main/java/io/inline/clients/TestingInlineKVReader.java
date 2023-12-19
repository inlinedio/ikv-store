package io.inline.clients;

import com.google.common.collect.ImmutableMap;
import com.inlineio.schemas.Common;
import com.inlineio.schemas.Streaming;
import io.inline.clients.internal.IKVClientJNI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.Nullable;

public class TestingInlineKVReader implements InlineKVReader, InlineKVWriter {
  private final DefaultInlineKVReader _defaultInlineKVReader;
  private final ClientOptions _options;

  public static final Map<String, Integer> FIELD_NAME_TO_FIELD_ID_MAPPING =
      new ImmutableMap.Builder<String, Integer>()
          .put("key", 0) // primary-key, bytes field
          .put("name", 1) // string field
          .put("profile", 2) // bytes field
          .build();

  public TestingInlineKVReader(ClientOptions options) {
    _options = options;
    _defaultInlineKVReader = new DefaultInlineKVReader(_options);
  }

  @Override
  public void startupReader() throws RuntimeException {
    // Reader Startup
    _defaultInlineKVReader.startupReader();
  }

  @Override
  public void startupWriter() {
    // Writer startup
    // Call reader once again
    startupWriter();
  }

  @Override
  public void shutdown() {}

  @Override
  public void shutdownReader() throws RuntimeException {
    _defaultInlineKVReader.shutdownReader();
  }

  @Override
  public void upsertFieldValues(IKVDocument document) {
    Map<String, Common.FieldValue> fieldValues = document.asMap();
    Common.IKVDocumentOnWire documentOnWire =
        Common.IKVDocumentOnWire.newBuilder().putAllDocument(fieldValues).build();

    Streaming.IKVDataEvent event =
        Streaming.IKVDataEvent.newBuilder()
            .setUpsertDocumentFieldsEvent(
                Streaming.UpsertDocumentFieldsEvent.newBuilder()
                    .setDocument(documentOnWire)
                    .build())
            .build();

    // jni call
    IKVClientJNI.processIKVDataEvent(_defaultInlineKVReader.handle(), event.toByteArray());
  }

  @Override
  public void deleteFieldValues(IKVDocument documentId, Collection<String> fieldsToDelete) {
    Map<String, Common.FieldValue> fieldValues = documentId.asMap();
    Common.IKVDocumentOnWire documentOnWire =
        Common.IKVDocumentOnWire.newBuilder().putAllDocument(fieldValues).build();

    Streaming.IKVDataEvent event =
        Streaming.IKVDataEvent.newBuilder()
            .setDeleteDocumentFieldsEvent(
                Streaming.DeleteDocumentFieldsEvent.newBuilder()
                    .setDocumentId(documentOnWire)
                    .addAllFieldsToDelete(fieldsToDelete)
                    .build())
            .build();

    // jni call
    IKVClientJNI.processIKVDataEvent(_defaultInlineKVReader.handle(), event.toByteArray());
  }

  @Override
  public void deleteDocument(IKVDocument documentId) {
    Map<String, Common.FieldValue> fieldValues = documentId.asMap();
    Common.IKVDocumentOnWire documentOnWire =
        Common.IKVDocumentOnWire.newBuilder().putAllDocument(fieldValues).build();

    Streaming.IKVDataEvent event =
        Streaming.IKVDataEvent.newBuilder()
            .setDeleteDocumentEvent(
                Streaming.DeleteDocumentEvent.newBuilder().setDocumentId(documentOnWire).build())
            .build();

    // jni call
    IKVClientJNI.processIKVDataEvent(_defaultInlineKVReader.handle(), event.toByteArray());
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
  public void batchDeleteFieldValues(
      Collection<IKVDocument> documentIds, Collection<String> fieldsToDelete) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void batchDeleteDocuments(Collection<IKVDocument> documentIds) {
    throw new UnsupportedOperationException();
  }
}
