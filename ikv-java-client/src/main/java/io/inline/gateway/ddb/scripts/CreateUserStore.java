package io.inline.gateway.ddb.scripts;

import com.google.common.base.Preconditions;
import io.inline.gateway.ddb.DynamoDBEnhancedClientFactory;
import io.inline.gateway.ddb.IKVStoreContextController;
import io.inline.gateway.ddb.beans.IKVStoreContext;
import java.util.Objects;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;

public class CreateUserStore {
  private static final Logger LOGGER = LogManager.getLogger(CreateUserStore.class);
  private static final TableSchema<IKVStoreContext> TABLE_SCHEMA =
      TableSchema.fromBean(IKVStoreContext.class);

  private final IKVStoreContextController _ikvStoreContextController;

  private CreateUserStore(IKVStoreContextController ikvStoreContextController) {
    _ikvStoreContextController = Objects.requireNonNull(ikvStoreContextController);
  }

  private void putItem(IKVStoreContext context) {
    Optional<IKVStoreContext> existingContext;
    try {
      existingContext =
          _ikvStoreContextController.getItem(context.getAccountId(), context.getStoreName());
    } catch (Exception e) {
      LOGGER.error("Cannot check if entry already exists.", e);
      return;
    }
    Preconditions.checkArgument(existingContext.isEmpty(), "item already exists, abort!");

    _ikvStoreContextController.putItem(context);
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
    IKVStoreContextController contextController =
        new IKVStoreContextController(DynamoDBEnhancedClientFactory.getClient());
    CreateUserStore createUserStore = new CreateUserStore(contextController);
    createUserStore.putItem(ikvStoreContext);
    LOGGER.info("Done.");
  }
}
