package io.inline.clients;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;

public final class LegacyIKVClient {
    private final long _indexHandle;

    public static LegacyIKVClient createNew(String mountPath, String schemaFilePath) {
        long indexHandle = IKVClientJNI.createNew(mountPath, schemaFilePath);
        return new LegacyIKVClient(indexHandle);
    }

    public static LegacyIKVClient open(String mountPath) {
        long indexHandle = IKVClientJNI.open(mountPath);
        return new LegacyIKVClient(indexHandle);
    }

    private LegacyIKVClient(long indexHandle) {
        _indexHandle = indexHandle;
    }

    public void close() {
        IKVClientJNI.close(_indexHandle);
    }

    public void upsertFieldValue(byte[] documentId, byte[] fieldValue, String fieldName) {
        IKVClientJNI.upsertFieldValues(_indexHandle, documentId, Collections.singletonList(fieldName),
                Collections.singletonList(fieldValue));
    }

    @Nullable
    public byte[] readBytesField(byte[] primaryKey, String fieldName) {
        return IKVClientJNI.readField(_indexHandle, primaryKey, fieldName);
    }

    @Nullable
    public String readStringField(byte[] primaryKey, String fieldName) {
        byte[] utf8Bytes = IKVClientJNI.readField(_indexHandle, primaryKey, fieldName);
        if (utf8Bytes == null) {
            return null;
        }

        return new String(utf8Bytes, StandardCharsets.UTF_8);
    }

    @Nullable
    public Integer readI32Field(byte[] primaryKey, String fieldName) {
        byte[] i32Bytes = IKVClientJNI.readField(_indexHandle, primaryKey, fieldName);
        if (i32Bytes == null) {
            return null;
        }

        return ByteBuffer.wrap(i32Bytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    public List<byte[]> batchReadBytesField(List<byte[]> primaryKeys, String fieldName) {
        if (primaryKeys == null || primaryKeys.size() == 0) {
            return Collections.emptyList();
        }

        // Concatenate and deep copy document ids
        int len = 0;
        for (byte[] primaryKey : primaryKeys) {
            len += 4 + primaryKey.length;
        }

        ByteBuffer bb = ByteBuffer.allocate(len).order(ByteOrder.LITTLE_ENDIAN);
        for (byte[] primaryKey : primaryKeys) {
            bb.putInt(primaryKey.length);
            bb.put(primaryKey);
        }

        // IKV lookup
        byte[] fieldValues =
                IKVClientJNI.batchReadField(_indexHandle, bb.array(), fieldName);

        // explode into individual byte arrays
        List<byte[]> values = new ArrayList<>(primaryKeys.size());

        bb = ByteBuffer.wrap(fieldValues).order(ByteOrder.LITTLE_ENDIAN);
        while (bb.hasRemaining()) {
            int size = bb.getInt();
            if (size == 0) {
                continue;
            }
            byte[] value = new byte[size];
            bb.get(value);
            values.add(value);
        }

        return values;
    }



    /**
     * Future APIs (one key, many fields)-

    public Map<String, byte[]> readBytesFields(byte[] primaryKey, List<String> fieldNames) {
        byte[] result = IKVClientJNI.readFields(_indexHandle, primaryKey, fieldNames);
        return unpack(result, fieldNames);
    }

    public Map<String, String> readStringFields(byte[] primaryKey, List<String> fieldNames) {
        byte[] result = IKVClientJNI.readFields(_indexHandle, primaryKey, fieldNames);
        Map<String, byte[]> unpacked = unpack(result, fieldNames);
        return Maps.transformValues(unpacked, utf8Bytes -> new String(utf8Bytes, StandardCharsets.UTF_8));
    }

    private static Map<String, byte[]> unpack(byte[] ikvResult, List<String> fieldNames) {
        Objects.requireNonNull(ikvResult);

        Map<String, byte[]> results = Maps.newHashMapWithExpectedSize(fieldNames.size());
        ByteBuffer bb = ByteBuffer.wrap(ikvResult).order(ByteOrder.LITTLE_ENDIAN);

        for (String fieldName : fieldNames) {
            int size = bb.getInt();
            if (size == 0) {
                continue;
            }
            byte[] value = new byte[size];
            bb.get(value);
            results.put(fieldName, value);
        }

        return results;
    }
     */
}
