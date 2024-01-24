package io.inlined.cloud.ddb;

import io.inlined.cloud.ddb.beans.IKVStoreContext;
import java.util.Objects;
import java.util.Optional;
import software.amazon.awssdk.enhanced.dynamodb.*;
import software.amazon.awssdk.enhanced.dynamodb.model.GetItemEnhancedRequest;

/** Access layer for DynamoDB table IKVStoreContextObjects. */
public class IKVStoreContextObjectsAccessor {
  private static final TableSchema<IKVStoreContext> TABLE_SCHEMA =
      TableSchema.fromBean(IKVStoreContext.class);
  private final DynamoDbTable<IKVStoreContext> _table;

  IKVStoreContextObjectsAccessor(DynamoDbEnhancedClient client) {
    DynamoDbTable<IKVStoreContext> table = client.table(IKVStoreContext.TABLE_NAME, TABLE_SCHEMA);
    _table = Objects.requireNonNull(table);
  }

  /**
   * Retrieve the entire store context object. Recommend to cache and only query when unknown fields
   * are found.
   *
   * @throws NullPointerException for null accountId or storeName
   */
  public Optional<IKVStoreContext> getItem(String accountId, String storeName) {
    Objects.requireNonNull(accountId);
    Objects.requireNonNull(storeName);
    Key primaryKey = Key.builder().partitionValue(accountId).sortValue(storeName).build();

    GetItemEnhancedRequest request =
        GetItemEnhancedRequest.builder().key(primaryKey).consistentRead(true).build();

    return Optional.ofNullable(_table.getItem(request));
  }

  /** Put store context. To be used for provisioning new stores. */
  public synchronized void putItem(IKVStoreContext ikvStoreContext) {
    Objects.requireNonNull(ikvStoreContext);
    _table.putItem(ikvStoreContext);
  }
}
