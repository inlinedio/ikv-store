package io.inline.clients;

import javax.annotation.Nullable;
import java.util.List;

public final class IKVClientJNI {
    // System.load("/home/ubuntu/inlineio/ikv/target/release/libikv.so");
    // System.load("/Users/pushkar/projects/inlineio/ikv/target/release/libikv.dylib");
    static {
        System.load("/Users/pushkar/projects/inlineio/ikv/target/release/libikv.dylib");
    }

    /** For simple testing. */
    public static native String provideHelloWorld();

    private IKVClientJNI() {
    }

    public static native long createNew(String mountPath, String schemaFilePath);
    public static native long open(String mountPath);
    public static native void close(long indexHandle);

    @Nullable
    static native byte[] getBytesFieldValue(long indexHandle, byte[] documentId, String fieldName);
    @Nullable
    static native byte[] getBatchBytesFieldValue(long indexHandle, byte[] documentIds, String fieldName);

    static native void getBatchBytesFieldValueV2(long indexHandle, List<byte[]> documentIds, String fieldName, List<byte[]> results);

    @Deprecated
    static native void upsertFieldValue(long indexHandle, byte[] documentId, byte[] fieldValue, String fieldName);

    static native void upsertFieldValues(long indexHandle, byte[] primaryKey, List<String> fieldNames, List<byte[]> fieldValues) throws RuntimeException;

    static native void deleteFieldValues(long indexHandle, byte[] primaryKey, List<String> fieldNames) throws RuntimeException;

    static native void deleteDocument(long indexHandle, byte[] primaryKey) throws RuntimeException;
}
