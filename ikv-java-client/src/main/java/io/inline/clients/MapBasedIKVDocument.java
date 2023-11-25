package io.inline.clients;

import com.inlineio.schemas.Services.FieldValue;

import java.util.Map;
import java.util.Objects;

class MapBasedIKVDocument implements IKVDocument {
    private final Map<String, FieldValue> _fields;

    public MapBasedIKVDocument(Map<String, FieldValue> fields) {
        _fields = Objects.requireNonNull(fields);
    }

    @Override
    public Map<String, FieldValue> asMap() {
        return _fields;
    }
}
