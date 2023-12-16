package io.inline.gateway;

import com.google.protobuf.InvalidProtocolBufferException;
import com.inlineio.schemas.Common.*;
import io.inline.gateway.ddb.beans.IKVStoreContext;
import io.inline.gateway.streaming.KafkaProducerFactory;
import java.util.*;
import javax.annotation.Nullable;

public class UserStoreContext {
  private final IKVStoreContext _ikvStoreContext;
  private final HashMap<String, FieldSchema> _schema;

  private UserStoreContext(IKVStoreContext ikvStoreContext, HashMap<String, FieldSchema> schema) {
    _ikvStoreContext = Objects.requireNonNull(ikvStoreContext);
    _schema = Objects.requireNonNull(schema);
  }

  public static UserStoreContext from(IKVStoreContext ikvStoreContext)
      throws InvalidProtocolBufferException {
    HashMap<String, FieldSchema> schema = new HashMap<>();

    // deserialize schema
    List<byte[]> serializedSchema = ikvStoreContext.getFieldSchema();
    for (byte[] bytes : serializedSchema) {
      FieldSchema fieldSchema = FieldSchema.parseFrom(bytes);
      schema.put(fieldSchema.getName(), fieldSchema);
    }

    return new UserStoreContext(ikvStoreContext, schema);
  }

  public String storeName() {
    return _ikvStoreContext.getStoreName();
  }

  public String kafkaTopic() {
    return _ikvStoreContext.getKafkaTopicName();
  }

  public int numKafkaPartitions() {
    return _ikvStoreContext.getNumPartitions();
  }

  public String primaryKeyFieldName() {
    // TODO: null handling for all field getters defined on _ikvStoreContext
    return _ikvStoreContext.getPrimaryKeyFieldName();
  }

  public String partitioningKeyFieldName() {
    return _ikvStoreContext.getPartitioningKeyFieldName();
  }

  /** Fetch schema for field given it's name. Returns empty for unknown/unregistered fields. */
  public Optional<FieldSchema> fieldSchema(String fieldName) {
    @Nullable FieldSchema schema = _schema.getOrDefault(fieldName, null);
    return Optional.ofNullable(schema);
  }

  public IKVStoreConfig createConfig() {
    String storeName = _ikvStoreContext.getStoreName();
    String accountId = _ikvStoreContext.getAccountId();

    return IKVStoreConfig.newBuilder()
        .putStringConfigs(IKVStoreConfigConstants.STORE_NAME, storeName)
        .putStringConfigs(IKVStoreConfigConstants.PRIMARY_KEY_FIELD_NAME, primaryKeyFieldName())
        .putStringConfigs(
            IKVStoreConfigConstants.PARTITIONING_KEY_FIELD_NAME, partitioningKeyFieldName())
        .putNumericConfigs(IKVStoreConfigConstants.NUM_KAFKA_PARTITIONS, numKafkaPartitions())
        .putStringConfigs(
            IKVStoreConfigConstants.KAFKA_CONSUMER_BOOTSTRAP_SERVER,
            KafkaProducerFactory.KAFKA_BOOTSTRAP_SERVER)
        .putStringConfigs(IKVStoreConfigConstants.KAFKA_CONSUMER_TOPIC_NAME, kafkaTopic())
        .putStringConfigs(
            IKVStoreConfigConstants.BASE_INDEX_S3_BUCKET_PREFIX,
            String.format("%s/%s", accountId, storeName))
        .putStringConfigs(IKVStoreConfigConstants.BASE_INDEX_S3_BUCKET_NAME, "ikv-base-indexes-v1")
        .build();
  }
}
