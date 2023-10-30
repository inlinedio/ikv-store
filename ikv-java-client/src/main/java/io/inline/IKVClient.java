package io.inline;

import javax.annotation.Nullable;
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

    @Nullable
    public byte[] getBytesFieldValue(byte[] documentId, String fieldName) {
        return IKVClientJNI.getBytesFieldValue(_indexHandle, documentId, fieldName);
    }

    public List<byte[]> getBatchBytesFieldValue(List<byte[]> documentIds, String fieldName) {
        return IKVClientJNI.getBatchBytesFieldValue(_indexHandle, documentIds, fieldName);
    }

    public void upsertFieldValue(byte[] documentId, byte[] fieldValue, String fieldName) {
        IKVClientJNI.upsertFieldValue(_indexHandle, documentId, fieldValue, fieldName);
    }
}
