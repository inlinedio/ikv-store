package io.inline.clients;

import javax.annotation.Nullable;
import java.util.List;

public interface InlineKVReader {
    void startup(ClientOptions options) throws RuntimeException;
    void shutdown() throws RuntimeException;

    @Nullable
    byte[] getBytesValue(PrimaryKey key, FieldAccessor fieldAccessor);

    List<byte[]> multiGetBytesValue(List<PrimaryKey> keys, FieldAccessor fieldAccessor);

    @Nullable
    String getStringValue(PrimaryKey key, FieldAccessor fieldAccessor);

    List<String> multiGetStringValue(List<PrimaryKey> keys, FieldAccessor fieldAccessor);


    // TODO - batch gets? Multi documents, multi fields
}
