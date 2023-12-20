package io.inline.clients;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.inlineio.schemas.Common;
import com.inlineio.schemas.Common.FieldValue;
import java.util.HashMap;
import java.util.Map;

/**
 * Data model which represents documents indexed in IKV. See: {@link IKVDocument.Builder} to
 * construct.
 */
public interface IKVDocument {

  Map<String, FieldValue> asNameToFieldValueMap();

  /**
   * Builder for creating {@link IKVDocument}s.
   *
   * <p>For every new document, a new builder should be instantiated and then fields can be added
   * with put***() methods.
   *
   * <p>Example -
   *
   * <pre>
   *  IKVDocument document = new IKVDocument.Builder()
   *      .putStringField("id", "primary-key-value")  // document primary key
   *      .putStringField("location", "partitioning-key-value") // document partitioning key
   *      .putStringField("feature1", "value1")
   *      .putBytesField("feature2", "value2".getBytes(StandardCharsets.UTF_8))
   *      ...
   *      .build();
   * </pre>
   */
  final class Builder {
    private final Map<String, FieldValue> _fields;

    public Builder() {
      _fields = new HashMap<>();
    }

    public Builder putStringField(String fieldName, String value) {
      Preconditions.checkArgument(!fieldName.isEmpty());
      Preconditions.checkArgument(!value.isEmpty());
      FieldValue fieldValue =
          FieldValue.newBuilder()
              .setFieldType(Common.FieldType.STRING)
              .setValue(ByteString.copyFromUtf8(value))
              .build();
      _fields.put(fieldName, fieldValue);
      return this;
    }

    public Builder putBytesField(String fieldName, byte[] value) {
      Preconditions.checkArgument(!fieldName.isEmpty());
      Preconditions.checkArgument(value.length != 0);
      FieldValue fieldValue =
          FieldValue.newBuilder()
              .setFieldType(Common.FieldType.BYTES)
              .setValue(ByteString.copyFrom(value))
              .build();
      _fields.put(fieldName, fieldValue);
      return this;
    }

    public IKVDocument build() {
      return new SimpleIKVDocument(_fields);
    }
  }
}
