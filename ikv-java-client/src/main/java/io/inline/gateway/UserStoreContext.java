package io.inline.gateway;

import com.google.common.base.Preconditions;
import com.inlineio.schemas.Common.*;
import io.inline.gateway.ddb.beans.IKVStoreContext;
import java.util.*;

/**
 * In-memory data model of a cached {@link IKVStoreContext} dynamodb bean. It represents all
 * metadata for a user provisioned IKV store.
 */
public class UserStoreContext {
  // TODO: consider storing this per store in dynamodb
  private static final String PUBLIC_SASL_SCRAM_KAFKA_BOOTSTRAP_SERVER =
      "b-1-public.mskcluster1.yz62h3.c5.kafka.us-west-2.amazonaws.com:9196,b-2-public.mskcluster1.yz62h3.c5.kafka.us-west-2.amazonaws.com:9196";
  private final IKVStoreContext _ikvStoreContext;

  private UserStoreContext(IKVStoreContext ikvStoreContext) {
    _ikvStoreContext = Objects.requireNonNull(ikvStoreContext);

    // validate required inner fields of _ikvStoreContext
    Preconditions.checkArgument(
        _ikvStoreContext.getStoreName() != null && !_ikvStoreContext.getStoreName().isEmpty());
    Preconditions.checkArgument(
        _ikvStoreContext.getKafkaTopicName() != null
            && !_ikvStoreContext.getKafkaTopicName().isEmpty());
    Preconditions.checkArgument(
        _ikvStoreContext.getNumPartitions() != null && _ikvStoreContext.getNumPartitions() > 0);
    Preconditions.checkArgument(
        _ikvStoreContext.getPrimaryKeyFieldName() != null
            && !_ikvStoreContext.getPrimaryKeyFieldName().isEmpty());
    Preconditions.checkArgument(
        _ikvStoreContext.getPartitioningKeyFieldName() != null
            && !_ikvStoreContext.getPartitioningKeyFieldName().isEmpty());
  }

  public static UserStoreContext from(IKVStoreContext ikvStoreContext) {
    return new UserStoreContext(ikvStoreContext);
  }

  public String accountId() {
    return _ikvStoreContext.getAccountId();
  }

  // sensitive field, do not log
  public String accountPasskey() {
    return _ikvStoreContext.getAccountPasskey();
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
    return _ikvStoreContext.getPrimaryKeyFieldName();
  }

  public String partitioningKeyFieldName() {
    return _ikvStoreContext.getPartitioningKeyFieldName();
  }

  public IKVStoreConfig createGatewaySpecifiedConfigs() {
    String storeName = _ikvStoreContext.getStoreName();
    String accountId = _ikvStoreContext.getAccountId();

    return IKVStoreConfig.newBuilder()
        .putStringConfigs(IKVConstants.ACCOUNT_ID, accountId)
        .putStringConfigs(IKVConstants.STORE_NAME, storeName)
        .putStringConfigs(IKVConstants.PRIMARY_KEY_FIELD_NAME, primaryKeyFieldName())
        .putStringConfigs(IKVConstants.PARTITIONING_KEY_FIELD_NAME, partitioningKeyFieldName())
        .putIntConfigs(IKVConstants.NUM_KAFKA_PARTITIONS, numKafkaPartitions())
        .putStringConfigs(
            IKVConstants.KAFKA_CONSUMER_BOOTSTRAP_SERVER, PUBLIC_SASL_SCRAM_KAFKA_BOOTSTRAP_SERVER)
        .putStringConfigs(IKVConstants.KAFKA_CONSUMER_TOPIC_NAME, kafkaTopic())
        .putStringConfigs(IKVConstants.BASE_INDEX_S3_BUCKET_NAME, IKVConstants.S3_BASE_INDEX_BUCKET)
        .build();
  }
}
