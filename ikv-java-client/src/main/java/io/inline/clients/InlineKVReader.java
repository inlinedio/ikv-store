package io.inline.clients;

import java.util.List;
import javax.annotation.Nullable;

public interface InlineKVReader {
  void startupReader() throws RuntimeException;

  void shutdownReader() throws RuntimeException;

  @Nullable
  byte[] getBytesValue(PrimaryKey key, FieldAccessor fieldAccessor);

  List<byte[]> multiGetBytesValue(List<PrimaryKey> keys, FieldAccessor fieldAccessor);

  @Nullable
  String getStringValue(PrimaryKey key, FieldAccessor fieldAccessor);

  List<String> multiGetStringValue(List<PrimaryKey> keys, FieldAccessor fieldAccessor);

  // TODO - batch gets? Multi documents, multi fields
}
