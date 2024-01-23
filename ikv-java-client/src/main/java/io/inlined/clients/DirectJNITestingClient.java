package io.inlined.clients;

import com.inlineio.schemas.Common;
import com.inlineio.schemas.Streaming;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

public class DirectJNITestingClient implements InlineKVReader, InlineKVWriter {
  private final DefaultInlineKVReader _defaultInlineKVReader;

  public DirectJNITestingClient(ClientOptions options) {
    _defaultInlineKVReader = new DefaultInlineKVReader(options, options.asIKVStoreConfig());
  }

  @Override
  public void startupReader() throws RuntimeException {
    // Reader Startup
    _defaultInlineKVReader.startupReader();
  }

  @Override
  public void startupWriter() {
    // Writer startup, no op
    _defaultInlineKVReader.startupReader();
  }

  @Override
  public void shutdownWriter() {}

  @Override
  public void shutdownReader() throws RuntimeException {
    _defaultInlineKVReader.shutdownReader();
  }

  @Override
  public void upsertFieldValues(IKVDocument document) {
    Map<String, Common.FieldValue> fieldValues = document.asNameToFieldValueMap();
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
    Map<String, Common.FieldValue> fieldValues = documentId.asNameToFieldValueMap();
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
    Map<String, Common.FieldValue> fieldValues = documentId.asNameToFieldValueMap();
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
  public byte[] getBytesValue(Object key, String fieldName) {
    return _defaultInlineKVReader.getBytesValue(key, fieldName);
  }

  @Override
  public List<byte[]> multiGetBytesValue(List<Object> keys, String fieldName) {
    return _defaultInlineKVReader.multiGetBytesValue(keys, fieldName);
  }

  @Nullable
  @Override
  public String getStringValue(Object key, String fieldName) {
    return _defaultInlineKVReader.getStringValue(key, fieldName);
  }

  @Override
  public List<String> multiGetStringValue(List<Object> keys, String fieldName) {
    return _defaultInlineKVReader.multiGetStringValue(keys, fieldName);
  }
}
