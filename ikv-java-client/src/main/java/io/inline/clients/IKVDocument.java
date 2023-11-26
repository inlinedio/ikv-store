package io.inline.clients;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.inlineio.schemas.Services.FieldValue;

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

        public Builder putStringField(String fieldName, String fieldValue) {
            Preconditions.checkArgument(!fieldName.isEmpty());
            Preconditions.checkArgument(!fieldValue.isEmpty());
            _fields.put(fieldName, FieldValue.newBuilder().setStringValue(fieldValue).build());
            return this;
        }

        public Builder putBytesField(String fieldName, byte[] fieldValue) {
            Preconditions.checkArgument(!fieldName.isEmpty());
            Preconditions.checkArgument(fieldValue.length != 0);
            _fields.put(fieldName, FieldValue.newBuilder().setBytesValue(ByteString.copyFrom(fieldValue)).build());
            return this;
        }

        public IKVDocument build() {
            return new MapBasedIKVDocument(_fields);
        }
    }
}
