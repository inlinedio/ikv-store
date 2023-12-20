package io.inline.clients;

import com.inlineio.schemas.Common.FieldValue;
import java.util.Map;
import java.util.Objects;

class SimpleIKVDocument implements IKVDocument {
  private final Map<String, FieldValue> _fields;

  public SimpleIKVDocument(Map<String, FieldValue> fields) {
    _fields = Objects.requireNonNull(fields);
  }

  @Override
  public Map<String, FieldValue> asNameToFieldValueMap() {
    return _fields;
  }
}
