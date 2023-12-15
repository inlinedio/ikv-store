package io.inline.gateway.ddb.scripts;

import com.google.common.base.Preconditions;
import com.inlineio.schemas.Common;
import io.inline.gateway.ddb.beans.IKVStoreContext;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class CreateUserStore {
  private static final Logger LOGGER = LogManager.getLogger(CreateUserStore.class);
  private static final TableSchema<IKVStoreContext> TABLE_SCHEMA =
      TableSchema.fromBean(IKVStoreContext.class);

  private final DynamoDbTable<IKVStoreContext> _table;

  private CreateUserStore() {
    DynamoDbEnhancedClient client =
        DynamoDbEnhancedClient.builder()
            .dynamoDbClient(
                DynamoDbClient.builder()
                    .endpointOverride(URI.create("http://localhost:8000"))
                    // .region(Region.US_EAST_1)
                    .credentialsProvider(ProfileCredentialsProvider.create())
                    .build())
            .build();
    _table = client.table(IKVStoreContext.TABLE_NAME, TABLE_SCHEMA);
  }

  private void putItem(IKVStoreContext context) {
    Key primaryKey =
        Key.builder()
            .partitionValue(context.getAccountId())
            .sortValue(context.getStoreName())
            .build();
    IKVStoreContext existingContext;
    try {
      existingContext = _table.getItem(primaryKey);
    } catch (Exception e) {
      LOGGER.error("Cannot check if entry already exists.", e);
      return;
    }
    Preconditions.checkArgument(existingContext == null, "item already exists, abort!");

    _table.putItem(context);
  }

  public static void main(String[] args) {
    // data
    String accountId = "testing-account-v1";
    String storeName = "testing-store";
    int numPartitions = 1;
    String kafkaTopicName = "testing-topic";
    String primaryKeyFieldName = "messageid";
    String partitioningKeyFieldName = "messagepartition";
    int fieldSchemaVersion = 2; // since we have 2 fields to start with
    // Construct schema
    List<byte[]> schema = new ArrayList<>();
    Common.FieldSchema primaryKeyField =
        Common.FieldSchema.newBuilder()
            .setName(primaryKeyFieldName)
            .setFieldType(Common.FieldType.STRING)
            .setId(0)
            .build();
    Common.FieldSchema partitioningKeyField =
        Common.FieldSchema.newBuilder()
            .setName(partitioningKeyFieldName)
            .setFieldType(Common.FieldType.STRING)
            .setId(1)
            .build();
    schema.add(primaryKeyField.toByteArray());
    schema.add(partitioningKeyField.toByteArray());

    IKVStoreContext ikvStoreContext = new IKVStoreContext();
    ikvStoreContext.setAccountId(accountId);
    ikvStoreContext.setStoreName(storeName);
    ikvStoreContext.setNumPartitions(numPartitions);
    ikvStoreContext.setKafkaTopicName(kafkaTopicName);
    ikvStoreContext.setPrimaryKeyFieldName(primaryKeyFieldName);
    ikvStoreContext.setPartitioningKeyFieldName(partitioningKeyFieldName);
    ikvStoreContext.setFieldSchemaVersion(fieldSchemaVersion);
    ikvStoreContext.setFieldSchema(schema);

    LOGGER.info("Inserting item: {}", ikvStoreContext);
    CreateUserStore createUserStore = new CreateUserStore();
    createUserStore.putItem(ikvStoreContext);
    LOGGER.info("Done.");
  }
}
