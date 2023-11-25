package io.inline.clients;

import com.google.common.base.Preconditions;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

public final class PrimaryKey {
    private final byte[] _key;  // serialized key

    private PrimaryKey(byte[] key) {
        _key = Objects.requireNonNull(key);
    }

    byte[] serializedKey() {
        return _key;
    }

    public static PrimaryKey from(byte[] key) {
        Preconditions.checkArgument(key != null && key.length != 0);
        return new PrimaryKey(key);
    }

    public static PrimaryKey from(String key) {
        Preconditions.checkArgument(key != null && !key.isEmpty());
        return new PrimaryKey(key.getBytes(StandardCharsets.UTF_8));
    }

    public static PrimaryKey from(int key) {
        throw new UnsupportedOperationException();
    }

    public static PrimaryKey from(long key) {
        throw new UnsupportedOperationException();
    }

    public static PrimaryKey from(float key) {
        throw new UnsupportedOperationException();
    }

    public static PrimaryKey from(double key) {
        throw new UnsupportedOperationException();
    }
}
