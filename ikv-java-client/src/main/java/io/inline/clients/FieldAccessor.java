package io.inline.clients;

import com.inlineio.schemas.Common.FieldType;

public class FieldAccessor {
    private final String _name;
    private final FieldType _type;

    public FieldAccessor(String name, FieldType type) {
        _name = name;
        _type = type;
    }

    public String fieldName() {
        return _name;
    }

    public FieldType fieldType() {
        return _type;
    }

    public static FieldAccessor stringFieldAccessor(String name) {
        return new FieldAccessor(name, FieldType.STRING);
    }

    public static FieldAccessor bytesFieldAccessor(String name) {
        return new FieldAccessor(name, FieldType.BYTES);
    }
}
