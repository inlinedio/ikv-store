package io.inline.clients;

import java.util.List;
import javax.annotation.Nullable;

public interface InlineKVReader {
  void startupReader() throws RuntimeException;

  void shutdownReader() throws RuntimeException;

  @Nullable
  byte[] getBytesValue(Object primaryKey, String fieldName);

  List<byte[]> multiGetBytesValue(List<Object> primaryKeys, String fieldName);

  @Nullable
  String getStringValue(Object primaryKey, String fieldName);

  List<String> multiGetStringValue(List<Object> primaryKeys, String fieldName);
}
