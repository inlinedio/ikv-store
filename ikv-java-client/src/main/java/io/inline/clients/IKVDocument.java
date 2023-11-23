package io.inline.clients;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.inlineio.schemas.Services.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class IKVDocument {
    private final Map<String, FieldValue> _fields;

    private IKVDocument(Map<String, FieldValue> fields) {
        _fields = Objects.requireNonNull(fields);
    }

    Map<String, FieldValue> accessFields() {
        return _fields;
    }

    private static final class Builder {
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

        public Builder putI32Field(String fieldName, int fieldValue) {
            throw new UnsupportedOperationException("todo");
        }

        public Builder putI64Field(String fieldName, long fieldValue) {
            throw new UnsupportedOperationException("todo");
        }

        public Builder putF32Field(String fieldName, float fieldValue) {
            throw new UnsupportedOperationException("todo");
        }

        public Builder putF64Field(String fieldName, double fieldValue) {
            throw new UnsupportedOperationException("todo");
        }

        public IKVDocument build() {
            return new IKVDocument(_fields);
        }
    }
}
