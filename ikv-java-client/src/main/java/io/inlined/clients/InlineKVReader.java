package io.inlined.clients;

import java.util.List;
import javax.annotation.Nullable;

public interface InlineKVReader {
  void startupReader() throws RuntimeException;

  void shutdownReader() throws RuntimeException;

  @Nullable
  byte[] getBytesValue(Object primaryKey, String fieldName);

  List<byte[]> multiGetBytesValues(List<Object> primaryKeys, String fieldName);

  @Nullable
  String getStringValue(Object primaryKey, String fieldName);

  List<String> multiGetStringValues(List<Object> primaryKeys, String fieldName);

  @Nullable
  Integer getIntValue(Object primaryKey, String fieldName);

  @Nullable
  Long getLongValue(Object primaryKey, String fieldName);

  @Nullable
  Float getFloatValue(Object primaryKey, String fieldName);

  @Nullable
  Double getDoubleValue(Object primaryKey, String fieldName);
}
