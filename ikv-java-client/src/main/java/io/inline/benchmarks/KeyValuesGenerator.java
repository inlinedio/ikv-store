package io.inline.benchmarks;

import com.google.common.base.Preconditions;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

public class KeyValuesGenerator {
    private final ArrayList<BytesKey> _keys;

    public KeyValuesGenerator(int numEntriesHint) {
        _keys = new ArrayList<>(numEntriesHint);
    }

    public BytesKey getKey(int index) {
        // lazily populate keys in field list
        for (int i = _keys.size() - 1; i < index; i++) {
            synchronized (this) {
                // curli braces required to ensure redis hashes are same.
                // see: https://repost.aws/knowledge-center/elasticache-crossslot-keys-error-redis
                String key = String.format("{key}-%d", i + 1);
                BytesKey bytesKey = new BytesKey(key.getBytes(StandardCharsets.UTF_8));
                _keys.add(bytesKey);
            }
        }

        return _keys.get(index);
    }

    public Collection<BytesKey> getKeyBatch(int startIndex, int endIndex) {
        Preconditions.checkArgument(startIndex <= endIndex);

        // lazily populate keys in field list
        for (int i = _keys.size() - 1; i < endIndex; i++) {
            String key = String.format("{key}-%d", i + 1);
            BytesKey bytesKey = new BytesKey(key.getBytes(StandardCharsets.UTF_8));
            _keys.add(bytesKey);
        }

        // a view over global list
        return _keys.subList(startIndex, endIndex);
    }

    public byte[] getValueBytes(int length, int seed) {
        byte[] result = new byte[length];
        Arrays.fill(result, (byte) seed);
        return result;
    }

    public final static class BytesKey {
        private final int _hash;
        private final byte[] _bytes;

        public BytesKey(byte[] bytes) {
            _hash = Arrays.hashCode(bytes);
            _bytes = bytes;
        }

        public byte[] getInnerBytes() {
            return _bytes;
        }

        @Override
        public int hashCode() {
            return _hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }

            if (this == obj) {
                return true;
            }

            if (!(obj instanceof BytesKey)) {
                return false;
            }

            BytesKey other = (BytesKey) obj;
            return Arrays.equals(_bytes, other._bytes);
        }
    }
}
