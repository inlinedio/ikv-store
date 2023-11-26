package io.inline.clients;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.inlineio.schemas.Common;
import io.inline.clients.internal.IKVClientJNI;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class DefaultInlineKVReader implements InlineKVReader {
    private static final long UNINITIALIZED_HANDLE = -1;
    private volatile long _handle;

    public DefaultInlineKVReader() {
        _handle = UNINITIALIZED_HANDLE;
    }

    @Override
    public void startup(ClientOptions options) throws RuntimeException {
        if (_handle != UNINITIALIZED_HANDLE) {
            // already running, ignore
            return;
        }

        try {
            _handle = IKVClientJNI.open(options.config().toByteArray());
        } catch (RuntimeException e) {
            System.out.println("Cannot create DefaultInlineKVReader due to: " + e);
            throw e;
        }
    }

    @Override
    public void shutdown() throws RuntimeException {
        IKVClientJNI.close(_handle);
        _handle = UNINITIALIZED_HANDLE;
    }

    @VisibleForTesting
    long handle() {
        return _handle;
    }

    @Nullable
    @Override
    public byte[] getBytesValue(PrimaryKey key, FieldAccessor fieldAccessor) {
        Preconditions.checkState(_handle != UNINITIALIZED_HANDLE);
        if (fieldAccessor.fieldType() != Common.FieldType.BYTES) {
            return null;
        }

        return IKVClientJNI.readField(_handle, key.serializedKey(), fieldAccessor.fieldName());
    }

    @Nullable
    @Override
    public String getStringValue(PrimaryKey key, FieldAccessor fieldAccessor) {
        Preconditions.checkState(_handle != UNINITIALIZED_HANDLE);
        if (fieldAccessor.fieldType() != Common.FieldType.STRING) {
            return null;
        }

        @Nullable byte[] result = IKVClientJNI.readField(_handle, key.serializedKey(), fieldAccessor.fieldName());
        return result == null ? null : new String(result, StandardCharsets.UTF_8);
    }

    @Override
    public List<byte[]> multiGetBytesValue(List<PrimaryKey> keys, FieldAccessor fieldAccessor) {
        Iterator<byte[]> result = multiGetRawByteValues(keys, fieldAccessor);
        if (!result.hasNext()) {
            return Collections.emptyList();
        }

        // drain into list, avoid Stream in hot path
        List<byte[]> results = new ArrayList<>(keys.size());
        while (result.hasNext()) {
            @Nullable byte[] next = result.next();
            results.add(next);
        }
        return results;
    }

    @Override
    public List<String> multiGetStringValue(List<PrimaryKey> keys, FieldAccessor fieldAccessor) {
        Iterator<byte[]> result = multiGetRawByteValues(keys, fieldAccessor);
        if (!result.hasNext()) {
            return Collections.emptyList();
        }

        // drain into list, avoid Stream in hot path
        List<String> results = new ArrayList<>(keys.size());
        while (result.hasNext()) {
            @Nullable byte[] next = result.next();
            results.add(next == null ? null : new String(next, StandardCharsets.UTF_8));
        }
        return results;
    }

    private Iterator<byte[]> multiGetRawByteValues(List<PrimaryKey> keys, FieldAccessor fieldAccessor) {
        if (keys == null || keys.isEmpty()) {
            return Collections.emptyIterator();
        }

        // always not null
        byte[] result = IKVClientJNI.batchReadField(
                _handle, sizePrefixedSerializedPrimaryKeys(keys), fieldAccessor.fieldName());

        return new RawByteValuesIterator(result);
    }

    private static final class RawByteValuesIterator implements Iterator<byte[]> {
        private final ByteBuffer _result;

        public RawByteValuesIterator(byte[] result) {
            _result = ByteBuffer.wrap(Objects.requireNonNull(result)).order(ByteOrder.LITTLE_ENDIAN);
        }

        @Override
        public boolean hasNext() {
            return _result.hasRemaining();
        }

        @Override
        @Nullable
        public byte[] next() {
            int size = _result.getInt();
            if (size == 0) {
                return null;
            }

            byte[] value = new byte[size];
            _result.get(value);
            return value;
        }
    }

    /**
     * Concatenates serialized bytes of primary-keys, by prefixing their
     * size as a lower-endian 32-bit signed integer.
     */
    private static byte[] sizePrefixedSerializedPrimaryKeys(List<PrimaryKey> keys) {
        int len = 0;
        for (PrimaryKey key : keys) {
            len += 4 + key.serializedKey().length;
        }

        ByteBuffer bb = ByteBuffer.allocate(len).order(ByteOrder.LITTLE_ENDIAN);
        for (PrimaryKey key : keys) {
            bb.putInt(key.serializedKey().length);
            bb.put(key.serializedKey());
        }

        return bb.array();
    }
}
