package io.inline.clients;

public class InlineKVReaderClient {
    private final long _indexHandle;

    public static InlineKVReaderClient open(String accountId, String passKey) {
        throw new UnsupportedOperationException();
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
