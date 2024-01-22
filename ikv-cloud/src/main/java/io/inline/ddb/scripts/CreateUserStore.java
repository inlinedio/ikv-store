package io.inline.ddb.scripts;

import com.google.common.base.Preconditions;
import io.inline.ddb.IKVStoreContextObjectsAccessor;
import io.inline.ddb.IKVStoreContextObjectsAccessorFactory;
import io.inline.ddb.beans.IKVStoreContext;
import java.util.Objects;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;

public class CreateUserStore {
  private static final Logger LOGGER = LogManager.getLogger(CreateUserStore.class);
  private static final TableSchema<IKVStoreContext> TABLE_SCHEMA =
      TableSchema.fromBean(IKVStoreContext.class);

  private final IKVStoreContextObjectsAccessor _ikvStoreContextObjectsAccessor;

  private CreateUserStore(IKVStoreContextObjectsAccessor ikvStoreContextObjectsAccessor) {
    _ikvStoreContextObjectsAccessor = Objects.requireNonNull(ikvStoreContextObjectsAccessor);
  }

  private void putItem(IKVStoreContext context) {
    Optional<IKVStoreContext> existingContext;
    try {
      existingContext =
          _ikvStoreContextObjectsAccessor.getItem(context.getAccountId(), context.getStoreName());
    } catch (Exception e) {
      LOGGER.error("Cannot check if entry already exists.", e);
      return;
    }
    Preconditions.checkArgument(existingContext.isEmpty(), "item already exists, abort!");

    _ikvStoreContextObjectsAccessor.putItem(context);
  }

  public static void main(String[] args) {
    // data
    String accountId = "testing-account-v1";
    String storeName = "testing-store";
    int numPartitions = 1;
    String kafkaTopicName = "testing-topic";
    String primaryKeyFieldName = "messageid";
    String partitioningKeyFieldName = "messagepartition";

    IKVStoreContext ikvStoreContext = new IKVStoreContext();
    ikvStoreContext.setAccountId(accountId);
    ikvStoreContext.setStoreName(storeName);
    ikvStoreContext.setNumPartitions(numPartitions);
    ikvStoreContext.setKafkaTopicName(kafkaTopicName);
    ikvStoreContext.setPrimaryKeyFieldName(primaryKeyFieldName);
    ikvStoreContext.setPartitioningKeyFieldName(partitioningKeyFieldName);

    LOGGER.info("Inserting item: {}", ikvStoreContext);
    IKVStoreContextObjectsAccessor contextController =
        IKVStoreContextObjectsAccessorFactory.getAccessor();
    CreateUserStore createUserStore = new CreateUserStore(contextController);
    createUserStore.putItem(ikvStoreContext);
    LOGGER.info("Done.");
  }
}
