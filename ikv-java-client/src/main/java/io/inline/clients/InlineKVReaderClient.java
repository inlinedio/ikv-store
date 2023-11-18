package io.inline.clients;

public class InlineKVReaderClient {
    private final long _indexHandle;

    public static InlineKVReaderClient open(String accountId, String passKey) {
        throw new UnsupportedOperationException();
    }

    public static InlineKVReaderClient createNew(String mountPath, String schemaFilePath) {
        long indexHandle = IKVClientJNI.createNew(mountPath, schemaFilePath);
        return new InlineKVReaderClient(indexHandle);
    }

    public static InlineKVReaderClient open(String mountPath) {
        long indexHandle = IKVClientJNI.open(mountPath);
        return new InlineKVReaderClient(indexHandle);
    }

    private InlineKVReaderClient(long indexHandle) {
        _indexHandle = indexHandle;
    }

    public void startup() {
        // Inspect on-disk index and download new if required

        // Start kafka consumption
        // Consumer<String, Streaming.IKVDataEvent> consumer;
        // consumer.seekToBeginning();
    }

    public void shutdown() {
    }
}
