package io.inline;

import com.google.common.collect.Maps;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

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
    public byte[] getBytesFieldValue(String documentId, String fieldName) {
        return IKVClientJNI.getFieldValue(_indexHandle, documentId.getBytes(StandardCharsets.UTF_8), fieldName);
    }

    public String getStringFieldValue(String documentId, String fieldName) {
        byte[] result = IKVClientJNI.getFieldValue(
                _indexHandle, documentId.getBytes(StandardCharsets.UTF_8), fieldName);
        if (result == null) {
            return null;
        }

        return new String(result, StandardCharsets.UTF_8);
    }

    public Map<String, String> getStringFieldValues(Collection<String> documentIds, String fieldName) {
        if (documentIds.size() == 0) {
            return Collections.emptyMap();
        }

        Map<String, String> result = Maps.newHashMapWithExpectedSize(documentIds.size());
        for (String documentId : documentIds) {
            byte[] value = IKVClientJNI.getFieldValue(
                    _indexHandle, documentId.getBytes(StandardCharsets.UTF_8), fieldName);
            if (value != null) {
                result.put(documentId, new String(value, StandardCharsets.UTF_8));
            }
        }

        return result;
    }

    public void upsertFieldValue(String documentId, byte[] fieldValue, String fieldName) {
        IKVClientJNI.upsertFieldValue(_indexHandle, documentId.getBytes(StandardCharsets.UTF_8), fieldValue, fieldName);
    }
}
