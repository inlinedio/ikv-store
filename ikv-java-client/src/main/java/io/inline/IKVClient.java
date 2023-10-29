package io.inline;

import java.nio.ByteBuffer;

public final class IKVClient {
    private final long _indexHandle;

    public static IKVClient create_new(String mountPath, String yamlSchema) {
        long indexHandle = IKVClientJNI.createNew(mountPath, yamlSchema);
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

    // nullable
    public ByteBuffer getFieldValue(byte[] documentId, String fieldName) {
        return IKVClientJNI.getBytesFieldValue(_indexHandle, documentId, fieldName);
    }

    public void upsertFieldValue(byte[] documentId, byte[] fieldValue, String fieldName) {
        IKVClientJNI.upsertFieldValue(_indexHandle, documentId, fieldValue, fieldName);
    }
}
