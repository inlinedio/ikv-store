package io.inline.benchmarks;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;

public class KeyValuesGenerator {
    private static final Random RANDOM = new Random();
    private final ArrayList<String> _keys;

    public KeyValuesGenerator(int numEntriesHint) {
        _keys = new ArrayList<>(numEntriesHint);
    }

    public String getKey(int index) {
        // lazily populate keys in field list
        for (int i = _keys.size() - 1; i < index; i++) {
            String key = String.format("key-%d", i + 1);
            _keys.add(key);
        }

        return _keys.get(index);
    }

    public Collection<String> getKeyBatch(int startIndex, int endIndex) {
        Preconditions.checkArgument(startIndex <= endIndex);

        // lazily populate keys in field list
        for (int i = _keys.size() - 1; i < endIndex; i++) {
            String key = String.format("key-%d", i + 1);
            _keys.add(key);
        }

        // a view over global list
        return _keys.subList(startIndex, endIndex);
    }

    public byte[] getValueBytes(int length) {
        byte[] result = new byte[length];
        RANDOM.nextBytes(result);
        return result;
    }
}
