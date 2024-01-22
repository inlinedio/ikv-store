package io.inline.ddb.beans;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

/**
 * Data model for a row in IKVStoreContextObjects DynamoDB table. Row holds all metadata like schema
 * and options to operate an IKV Store.
 *
 * <p>See annotations for partitioning and sort keys.
 */
@DynamoDbBean
public class IKVStoreContext {
  public static final String TABLE_NAME = "IKVStoreContextObjects";

  private String AccountId;
  private String StoreName;
  private String AccountPasskey; // sensitive field
  private Integer NumPartitions;
  private String KafkaTopicName;

  private String PrimaryKeyFieldName;
  private String PartitioningKeyFieldName;

  @DynamoDbPartitionKey
  @DynamoDbAttribute("AccountId")
  public String getAccountId() {
    return AccountId;
  }

  public void setAccountId(String accountId) {
    this.AccountId = accountId;
  }

  @DynamoDbSortKey
  @DynamoDbAttribute("StoreName")
  public String getStoreName() {
    return StoreName;
  }

  public void setStoreName(String storeName) {
    this.StoreName = storeName;
  }

  @DynamoDbAttribute("AccountPasskey")
  public String getAccountPasskey() {
    return AccountPasskey;
  }

  public void setAccountPasskey(String accountPasskey) {
    this.AccountPasskey = accountPasskey;
  }

  @DynamoDbAttribute("NumPartitions")
  public Integer getNumPartitions() {
    return NumPartitions;
  }

  public void setNumPartitions(Integer numPartitions) {
    this.NumPartitions = numPartitions;
  }

  @DynamoDbAttribute("KafkaTopicName")
  public String getKafkaTopicName() {
    return KafkaTopicName;
  }

  public void setKafkaTopicName(String kafkaTopicName) {
    this.KafkaTopicName = kafkaTopicName;
  }

  @DynamoDbAttribute("PrimaryKeyFieldName")
  public String getPrimaryKeyFieldName() {
    return PrimaryKeyFieldName;
  }

  public void setPrimaryKeyFieldName(String primaryKeyFieldName) {
    this.PrimaryKeyFieldName = primaryKeyFieldName;
  }

  @DynamoDbAttribute("PartitioningKeyFieldName")
  public String getPartitioningKeyFieldName() {
    return PartitioningKeyFieldName;
  }

  public void setPartitioningKeyFieldName(String partitioningKeyFieldName) {
    this.PartitioningKeyFieldName = partitioningKeyFieldName;
  }

  @Override
  public String toString() {
    return "IKVStoreContext{"
        + "AccountId='"
        + AccountId
        + '\''
        + ", StoreName='"
        + StoreName
        + '\''
        + ", NumPartitions="
        + NumPartitions
        + ", KafkaTopicName='"
        + KafkaTopicName
        + '\''
        + ", PrimaryKeyFieldName='"
        + PrimaryKeyFieldName
        + '\''
        + ", PartitioningKeyFieldName='"
        + PartitioningKeyFieldName
        + '\''
        + '}';
  }
}
