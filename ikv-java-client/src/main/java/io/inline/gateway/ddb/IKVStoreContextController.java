package io.inline.gateway.ddb;

import io.inline.gateway.ddb.beans.IKVStoreContext;
import java.util.Objects;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.enhanced.dynamodb.*;

public class IKVStoreContextController {
  private static final Logger LOGGER = LogManager.getLogger(IKVStoreContextController.class);
  private static final TableSchema<IKVStoreContext> TABLE_SCHEMA =
      TableSchema.fromBean(IKVStoreContext.class);
  private final DynamoDbTable<IKVStoreContext> _table;

  // TODO: make factory for this
  public IKVStoreContextController(DynamoDbEnhancedClient client) {
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

    // TODO: use GetItemEnhancedRequest and always use strongly consistent read.
    return Optional.ofNullable(_table.getItem(primaryKey));
  }

  public synchronized void putItem(IKVStoreContext ikvStoreContext) {
    Objects.requireNonNull(ikvStoreContext);
    _table.putItem(ikvStoreContext);
  }
}
