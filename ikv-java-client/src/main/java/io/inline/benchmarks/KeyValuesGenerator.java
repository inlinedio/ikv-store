package io.inline.benchmarks;

import com.google.common.base.Preconditions;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;

public class KeyValuesGenerator {
    private static final int VALUE_LENGTH = 1000;
    public static final byte[] VALUE_BYTES;
    public static final String VALUE_BYTES_STRING;
    static {
        VALUE_BYTES = new byte[VALUE_LENGTH];
        for (int i = 0; i < VALUE_LENGTH; i++) {
            VALUE_BYTES[i] = (byte) i;
        }
        VALUE_BYTES_STRING = new String(VALUE_BYTES, StandardCharsets.UTF_8);
    }

    private final ArrayList<String> _keys;

    public KeyValuesGenerator(int numEntriesHint) {
        _keys = new ArrayList<>(numEntriesHint);
    }

    public String getKey(int index) {
        for (int i = _keys.size() - 1; i < index; i++) {
            String key = String.format("key-%d", i + 1);
            _keys.add(key);
        }

        return _keys.get(index);
    }

    public Collection<String> getKeyBatch(int startIndex, int endIndex) {
        Preconditions.checkArgument(startIndex <= endIndex);
        for (int i = _keys.size() - 1; i < endIndex; i++) {
            String key = String.format("key-%d", i + 1);
            _keys.add(key);
        }

        // a view over global list
        return _keys.subList(startIndex, endIndex);
    }
}
