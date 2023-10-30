package io.inline;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;

public final class IKVClientJNI {
    static {
        System.load("/Users/pushkar/projects/inlineio/ikv/target/debug/libikv.dylib");
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
    static native List<byte[]> getBatchBytesFieldValue(long indexHandle, List<byte[]> documentIds, String fieldName);

    static native void upsertFieldValue(long indexHandle, byte[] documentId, byte[] fieldValue, String fieldName);
}
