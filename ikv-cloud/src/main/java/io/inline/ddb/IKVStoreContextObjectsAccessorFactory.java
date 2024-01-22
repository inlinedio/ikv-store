package io.inline.ddb;

import javax.annotation.Nullable;

public class IKVStoreContextObjectsAccessorFactory {
  @Nullable private static volatile IKVStoreContextObjectsAccessor ACCESSOR = null;

  public IKVStoreContextObjectsAccessorFactory() {}

  public static synchronized IKVStoreContextObjectsAccessor getAccessor() {
    if (ACCESSOR == null) {
      ACCESSOR = new IKVStoreContextObjectsAccessor(DynamoDBEnhancedClientFactory.getClient());
    }
    return ACCESSOR;
  }
}
