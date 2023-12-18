package io.inline.gateway.ddb;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import com.inlineio.schemas.Common;
import io.inline.gateway.ddb.beans.IKVStoreContext;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.enhanced.dynamodb.*;
import software.amazon.awssdk.enhanced.dynamodb.model.UpdateItemEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;

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

  /**
   * Add new field's schema to an IKV store. Does not throw exception if this field is already
   * tracked in the context ie was registered previously.
   *
   * @return true - if field was registered successfully false - field already exists or there was
   *     an error (retries exhausted)
   * @throws NullPointerException if any input args are null
   * @throws IllegalStateException if stored IKVStoreContext object in DynamoDB cannot be parsed
   * @throws InterruptedException if thread sleep b/w retries is interrupted - ok to call this
   *     method again
   */
  @Deprecated
  public synchronized boolean registerSchemaForNewField(
      String accountId, String storeName, Common.FieldSchema field) throws InterruptedException {
    Preconditions.checkNotNull(accountId);
    Preconditions.checkNotNull(storeName);
    Preconditions.checkNotNull(field);

    Preconditions.checkArgument(
        field.getFieldType() != Common.FieldType.UNKNOWN, "Cannot add field with unknown type");

    Key primaryKey = Key.builder().partitionValue(accountId).sortValue(storeName).build();

    // conditional update w/ retries
    for (int retry = 0; retry < 5; retry++) {
      @Nullable IKVStoreContext ikvStoreContext = _table.getItem(primaryKey);
      Preconditions.checkNotNull(
          ikvStoreContext,
          String.format(
              "IKVStoreContext does not exist for accountId: %s storeName: %s. "
                  + "Cannot add field: %s field-schema",
              accountId, storeName, field.getName()));

      // check if schema already exists
      for (byte[] bytes : ikvStoreContext.getFieldSchema()) {
        try {
          Common.FieldSchema existingFieldSchema = Common.FieldSchema.parseFrom(bytes);
          if (field.getName().equals(existingFieldSchema.getName())) {
            // this field already exists, not an error, early return
            LOGGER.info(
                "Ignoring field registration since it already exists, Field: {} AccountId: {} StoreName: {}",
                field.getName(),
                accountId,
                storeName);
            return false;
          }
        } catch (InvalidProtocolBufferException e) {
          throw new IllegalStateException("Cannot deserialize existing schema. Error: ", e);
        }
      }

      // update schema and schema-version in existing IKVStoreContext
      int existingSchemaVersion = ikvStoreContext.getFieldSchemaVersion();
      Common.FieldSchema newFieldSchema =
          Common.FieldSchema.newBuilder()
              .setName(field.getName())
              .setId(existingSchemaVersion)
              .setFieldType(field.getFieldType())
              .build();
      byte[] serializedNewFieldSchema = newFieldSchema.toByteArray();

      // Mutate ikvStoreContext
      ikvStoreContext.getFieldSchema().add(serializedNewFieldSchema);
      ikvStoreContext.setFieldSchemaVersion(existingSchemaVersion + 1);

      // Transactional update - using schema version as conditional variable
      UpdateItemEnhancedRequest<IKVStoreContext> request =
          UpdateItemEnhancedRequest.builder(IKVStoreContext.class)
              .item(ikvStoreContext)
              .conditionExpression(
                  Expression.builder()
                      .expression("#schemaVersion = :schemaVersionValue")
                      .putExpressionName("#schemaVersion", "fieldSchemaVersion")
                      .putExpressionValue(
                          ":schemaVersionValue",
                          AttributeValue.fromN(String.valueOf(existingSchemaVersion)))
                      .build())
              .build();

      try {
        _table.updateItem(request);
      } catch (ConditionalCheckFailedException e) {
        LOGGER.info(
            "Retrying due to version conflict while updating schema for "
                + "accountId: {} store: {} exception: {}",
            accountId,
            storeName,
            e);
        Thread.sleep(1000 * 2); // 2s sleep
        continue;
      }

      // Success!
      LOGGER.info(
          "Successfully registered new field: {} to accountId: {} storeName: {}",
          field.getName(),
          accountId,
          storeName);
      return true;
    }

    LOGGER.error(
        "Cannot register after retries new field: {} to accountId: {} storeName: {}",
        field.getName(),
        accountId,
        storeName);
    return false;
  }
}
