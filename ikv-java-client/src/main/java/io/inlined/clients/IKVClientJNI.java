package io.inlined.clients;

import java.io.IOException;
import java.util.Optional;
import javax.annotation.Nullable;

public final class IKVClientJNI {
  // can throw UnsatisfiedLinkError for a wrong path or incompatible binary
  private IKVClientJNI(String pathToNativeBinary) {
    System.load(pathToNativeBinary);
  }

  public static IKVClientJNI createNew(String mountDirectory) throws IOException {
    NativeBinaryManager nativeBinaryManager = new NativeBinaryManager(mountDirectory);
    try {
      Optional<String> maybePath = nativeBinaryManager.getPathToNativeBinary();
      if (maybePath.isEmpty()) {
        throw new IOException(
            "Could not find a native binary for reader instance, for this os/platform");
      }

      return new IKVClientJNI(maybePath.get());
    } finally {
      nativeBinaryManager.close();
    }
  }

  public static void main(String[] args) {
    // /home/ubuntu/ikv-store/ikv/target/release/libikv.so
    // /Users/pushkar/projects/ikv-store/ikv/target/release/libikv.dylib
    IKVClientJNI ikvClientJNI = new IKVClientJNI(args[0]);
    System.out.println(ikvClientJNI.provideHelloWorld());
  }

  /** For linkage testing. */
  public native String provideHelloWorld();

  // Open or create.
  // config: Serialized IKVStoreConfig.proto
  // RuntimeException: opening errors.
  public native long open(byte[] config) throws RuntimeException;

  public native void close(long indexHandle) throws RuntimeException;

  @Nullable
  public native byte[] readField(long indexHandle, byte[] primaryKey, String fieldName);

  public native byte[] batchReadField(
      long indexHandle, byte[] sizePrefixedPrimaryKeys, String fieldNames);

  /** Write method - only for testing. */
  public native void processIKVDataEvent(long indexHandle, byte[] ikvDataEvent)
      throws RuntimeException;

  /**
   * Hook to build index by consuming nearline event stream. Index is built in-place of existing
   * base index present in the mount directory.
   */
  public native void buildIndex(byte[] config) throws RuntimeException;
}
