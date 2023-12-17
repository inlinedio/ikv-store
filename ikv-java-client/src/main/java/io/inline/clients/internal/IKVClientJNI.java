package io.inline.clients.internal;

import javax.annotation.Nullable;

public final class IKVClientJNI {
  // System.load("/home/ubuntu/inlineio/ikv/target/release/libikv.so");
  // System.load("/Users/pushkar/projects/inlineio/ikv/target/release/libikv.dylib");
  static {
    System.load("/Users/pushkar/projects/inlineio/ikv/target/release/libikv.dylib");
  }

  /** For simple testing. */
  public static native String provideHelloWorld();

  private IKVClientJNI() {}

  public static void main(String[] args) {
    // for testing any linkage errors
    String output = IKVClientJNI.provideHelloWorld();
    System.out.println(output);
  }

  // Open or create.
  // config: Serialized IKVStoreConfig.proto
  // RuntimeException: opening errors.
  public static native long open(byte[] config) throws RuntimeException;

  public static native void close(long indexHandle) throws RuntimeException;

  @Nullable
  public static native byte[] readField(long indexHandle, byte[] primaryKey, String fieldName);

  public static native byte[] batchReadField(
      long indexHandle, byte[] sizePrefixedPrimaryKeys, String fieldNames);

  /** Write method - only for testing. */
  public static native void processIKVDataEvent(long indexHandle, byte[] ikvDataEvent)
      throws RuntimeException;

  /**
   * Hook to build index by consuming nearline event stream. Index is built in-place of existing
   * base index present in the mount directory.
   */
  public static native void buildIndex(byte[] config) throws RuntimeException;
}
