package io.inline;

import javax.annotation.Nullable;
import java.util.List;

public final class IKVClientJNI {
    // System.load("/home/ubuntu/inlineio/ikv/target/release/libikv.so");
    // System.load("/Users/pushkar/projects/inlineio/ikv/target/debug/libikv.dylib");
    static {
        System.load("/home/ubuntu/inlineio/ikv/target/release/libikv.so");
    }

    /** For simple testing. */
    public static native String provideHelloWorld();

    private IKVClientJNI() {
    }

    static native long createNew(String mountPath, String schemaFilePath);
    static native long open(String mountPath);
    static native void close(long indexHandle);

    @Nullable
    static native byte[] getBytesFieldValue(long indexHandle, byte[] documentId, String fieldName);
    @Nullable
    static native byte[] getBatchBytesFieldValue(long indexHandle, byte[] documentIds, String fieldName);

    static native void getBatchBytesFieldValueV2(long indexHandle, List<byte[]> documentIds, String fieldName, List<byte[]> results);

    static native void upsertFieldValue(long indexHandle, byte[] documentId, byte[] fieldValue, String fieldName);
}
