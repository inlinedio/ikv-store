package io.inlined.clients;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.inlineio.schemas.Common;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import javax.annotation.Nullable;

public class DefaultInlineKVReader implements InlineKVReader {
  private static final long UNINITIALIZED_HANDLE = -1;
  private final ClientOptions _clientOptions;
  private final Common.IKVStoreConfig _clientServerMergedConfig;
  private volatile long _handle;

  public DefaultInlineKVReader(
      ClientOptions options, Common.IKVStoreConfig clientServerMergedConfig) {
    _handle = UNINITIALIZED_HANDLE;
    _clientOptions = Objects.requireNonNull(options);
    _clientServerMergedConfig = clientServerMergedConfig;
  }

  @Override
  public void startupReader() throws RuntimeException {
    if (_handle != UNINITIALIZED_HANDLE) {
      return;
    }

    // can throw
    _handle = IKVClientJNI.open(_clientServerMergedConfig.toByteArray());
  }

  @Override
  public void shutdownReader() throws RuntimeException {
    // can throw
    IKVClientJNI.close(_handle);
    _handle = UNINITIALIZED_HANDLE;
  }

  @VisibleForTesting
  long handle() {
    return _handle;
  }

  @Nullable
  @Override
  public byte[] getBytesValue(Object primaryKey, String fieldName) {
    Preconditions.checkState(_handle != UNINITIALIZED_HANDLE);
    return IKVClientJNI.readField(
        _handle, serializePrimaryKey(primaryKey, _clientOptions.primaryKeyType()), fieldName);
  }

  @Nullable
  @Override
  public String getStringValue(Object primaryKey, String fieldName) {
    Preconditions.checkState(_handle != UNINITIALIZED_HANDLE);

    @Nullable
    byte[] result =
        IKVClientJNI.readField(
            _handle, serializePrimaryKey(primaryKey, _clientOptions.primaryKeyType()), fieldName);
    return result == null ? null : new String(result, StandardCharsets.UTF_8);
  }

  @Override
  public List<byte[]> multiGetBytesValue(List<Object> primaryKeys, String fieldName) {
    Iterator<byte[]> result = multiGetRawByteValues(primaryKeys, fieldName);
    if (!result.hasNext()) {
      return Collections.emptyList();
    }

    // drain into list, avoid Stream in hot path
    List<byte[]> results = new ArrayList<>(primaryKeys.size());
    while (result.hasNext()) {
      @Nullable byte[] next = result.next();
      results.add(next);
    }
    return results;
  }

  @Override
  public List<String> multiGetStringValue(List<Object> primaryKeys, String fieldName) {
    Iterator<byte[]> result = multiGetRawByteValues(primaryKeys, fieldName);
    if (!result.hasNext()) {
      return Collections.emptyList();
    }

    // drain into list, avoid Stream in hot path
    List<String> results = new ArrayList<>(primaryKeys.size());
    while (result.hasNext()) {
      @Nullable byte[] next = result.next();
      results.add(next == null ? null : new String(next, StandardCharsets.UTF_8));
    }
    return results;
  }

  private Iterator<byte[]> multiGetRawByteValues(List<Object> primaryKeys, String fieldName) {
    if (primaryKeys == null || primaryKeys.isEmpty()) {
      return Collections.emptyIterator();
    }

    // always not null
    byte[] sizePrefixedPrimaryKeys =
        sizePrefixedSerializedPrimaryKeys(primaryKeys, _clientOptions.primaryKeyType());
    byte[] result = IKVClientJNI.batchReadField(_handle, sizePrefixedPrimaryKeys, fieldName);
    return new RawByteValuesIterator(result);
  }

  private static final class RawByteValuesIterator implements Iterator<byte[]> {
    private final ByteBuffer _result;

    public RawByteValuesIterator(byte[] result) {
      _result = ByteBuffer.wrap(Objects.requireNonNull(result)).order(ByteOrder.LITTLE_ENDIAN);
    }

    @Override
    public boolean hasNext() {
      return _result.hasRemaining();
    }

    @Override
    @Nullable
    public byte[] next() {
      int size = _result.getInt();
      if (size == 0) {
        return null;
      }

      byte[] value = new byte[size];
      _result.get(value);
      return value;
    }
  }

  /**
   * Concatenates serialized bytes of primary-keys, by prefixing their size as a lower-endian 32-bit
   * signed integer.
   */
  private static byte[] sizePrefixedSerializedPrimaryKeys(
      List<Object> primaryKeys, Common.FieldType fieldType) {
    if (primaryKeys == null || primaryKeys.isEmpty()) {
      return new byte[0];
    }

    int capacity = 0;

    int i = 0;
    byte[][] serializedPrimaryKeys = new byte[primaryKeys.size()][];
    for (Object primaryKey : primaryKeys) {
      byte[] serializedPrimaryKey = serializePrimaryKey(primaryKey, fieldType);
      capacity += 4 + serializedPrimaryKey.length;

      serializedPrimaryKeys[i] = serializedPrimaryKey;
      i++;
    }

    ByteBuffer bb = ByteBuffer.allocate(capacity).order(ByteOrder.LITTLE_ENDIAN);
    for (byte[] serializedPrimaryKey : serializedPrimaryKeys) {
      bb.putInt(serializedPrimaryKey.length);
      bb.put(serializedPrimaryKey);
    }

    return bb.array();
  }

  private static byte[] serializePrimaryKey(Object primaryKey, Common.FieldType fieldType) {
    switch (fieldType) {
      case STRING -> {
        return primaryKey.toString().getBytes(StandardCharsets.UTF_8);
      }
      case BYTES -> {
        // can throw ClassCastException - ok
        return (byte[]) primaryKey;
      }
      default -> throw new UnsupportedOperationException();
    }
  }
}
