package io.inline;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class IKVClient {
    private final long _indexHandle;

    public static IKVClient create_new(String mountPath, String schemaFilePath) {
        long indexHandle = IKVClientJNI.createNew(mountPath, schemaFilePath);
        return new IKVClient(indexHandle);
    }

    public static IKVClient open(String mountPath) {
        long indexHandle = IKVClientJNI.open(mountPath);
        return new IKVClient(indexHandle);
    }

    private IKVClient(long indexHandle) {
        _indexHandle = indexHandle;
    }

    public void close() {
        IKVClientJNI.close(_indexHandle);
    }

    public void upsertFieldValue(byte[] documentId, byte[] fieldValue, String fieldName) {
        IKVClientJNI.upsertFieldValue(_indexHandle, documentId, fieldValue, fieldName);
    }

    @Nullable
    public byte[] getBytesFieldValue(byte[] documentId, String fieldName) {
        return IKVClientJNI.getBytesFieldValue(_indexHandle, documentId, fieldName);
    }

    public List<byte[]> getBatchBytesFieldValueV2(List<byte[]> documentIds, String fieldName) {
        if (documentIds == null || documentIds.size() == 0) {
            return Collections.emptyList();
        }

        List<byte[]> results = new ArrayList<>(documentIds.size());
        IKVClientJNI.getBatchBytesFieldValueV2(_indexHandle, documentIds, fieldName, results);

        return results;
    }

    public List<byte[]> getBatchBytesFieldValue(List<byte[]> documentIds, String fieldName) {
        if (documentIds == null || documentIds.size() == 0) {
            return Collections.emptyList();
        }

        // Concatenate and deep copy document ids
        int len = 0;
        for (byte[] docid : documentIds) {
            len += 4 + docid.length;
        }

        ByteBuffer bb = ByteBuffer.allocate(len);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        for (byte[] docid : documentIds) {
            bb.putInt(docid.length);
            bb.put(docid);
        }

        // IKV lookup
        @Nullable byte[] fieldValues =
                IKVClientJNI.getBatchBytesFieldValue(_indexHandle, bb.array(), fieldName);
        if (fieldValues == null || fieldValues.length == 0) {
            return Collections.emptyList();
        }

        // explode into individual byte arrays
        List<byte[]> result = new ArrayList<>(documentIds.size());

        bb = ByteBuffer.wrap(fieldValues);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        while (bb.hasRemaining()) {
            int size = bb.getInt();
            byte[] fieldValue = new byte[size];
            bb.get(fieldValue, 0, size);
            result.add(fieldValue);
        }

        return result;
    }
}
