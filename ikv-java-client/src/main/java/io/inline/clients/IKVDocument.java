package io.inline.clients;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.inlineio.schemas.Common;
import com.inlineio.schemas.Common.FieldValue;

import java.util.HashMap;
import java.util.Map;

public interface IKVDocument {

    @Deprecated
    Map<String, FieldValue> asMap();

    final class Builder {
        private final Map<String, FieldValue> _fields;

        public Builder() {
            _fields = new HashMap<>();
        }

        public Builder putStringField(String fieldName, String value) {
            Preconditions.checkArgument(!fieldName.isEmpty());
            Preconditions.checkArgument(!value.isEmpty());
            FieldValue fieldValue = FieldValue.newBuilder()
                    .setFieldType(Common.FieldType.STRING)
                    .setValue(ByteString.copyFromUtf8(value))
                    .build();
            _fields.put(fieldName, fieldValue);
            return this;
        }

        public Builder putBytesField(String fieldName, byte[] value) {
            Preconditions.checkArgument(!fieldName.isEmpty());
            Preconditions.checkArgument(value.length != 0);
            FieldValue fieldValue = FieldValue.newBuilder()
                    .setFieldType(Common.FieldType.BYTES)
                    .setValue(ByteString.copyFrom(value))
                    .build();
            _fields.put(fieldName, fieldValue);
            return this;
        }

        public IKVDocument build() {
            return new MapBasedIKVDocument(_fields);
        }
    }
}
