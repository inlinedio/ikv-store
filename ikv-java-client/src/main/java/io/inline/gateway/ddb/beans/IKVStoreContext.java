package io.inline.gateway.ddb.beans;


import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

import java.util.List;

/**
 * Data model for a row in IKVStoreContextObjects DynamoDB table.
 * Row holds all metadata like schema and options to operate an IKV Store.
 *
 * Primary Key: PartitioningKey - accountId, SortKey - storeName
 */

@DynamoDbBean
public class IKVStoreContext {
    public static final String TABLE_NAME = "IKVStoreContextObjects";

    private String AccountId;
    private String StoreName;
    private Integer NumPartitions;
    private String KafkaTopicName;

    private String PrimaryKeyFieldName;
    private String PartitioningKeyFieldName;

    // List of serialized FieldSchema.proto objects
    private List<byte[]> FieldSchema;
    private Integer FieldSchemaVersion;  // starts from 1

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

    public Integer getNumPartitions() {
        return NumPartitions;
    }

    public void setNumPartitions(Integer numPartitions) {
        this.NumPartitions = numPartitions;
    }

    public String getKafkaTopicName() {
        return KafkaTopicName;
    }

    public void setKafkaTopicName(String kafkaTopicName) {
        this.KafkaTopicName = kafkaTopicName;
    }

    public String getPrimaryKeyFieldName() {
        return PrimaryKeyFieldName;
    }

    public void setPrimaryKeyFieldName(String primaryKeyFieldName) {
        this.PrimaryKeyFieldName = primaryKeyFieldName;
    }

    public String getPartitioningKeyFieldName() {
        return PartitioningKeyFieldName;
    }

    public void setPartitioningKeyFieldName(String partitioningKeyFieldName) {
        this.PartitioningKeyFieldName = partitioningKeyFieldName;
    }

    public List<byte[]> getFieldSchema() {
        return FieldSchema;
    }

    public void setFieldSchema(List<byte[]> fieldSchema) {
        this.FieldSchema = fieldSchema;
    }

    public Integer getFieldSchemaVersion() {
        return FieldSchemaVersion;
    }

    public void setFieldSchemaVersion(Integer fieldSchemaVersion) {
        this.FieldSchemaVersion = fieldSchemaVersion;
    }

    @Override
    public String toString() {
        return "IKVStoreContext{" +
                "AccountId='" + AccountId + '\'' +
                ", StoreName='" + StoreName + '\'' +
                ", NumPartitions=" + NumPartitions +
                ", KafkaTopicName='" + KafkaTopicName + '\'' +
                ", PrimaryKeyFieldName='" + PrimaryKeyFieldName + '\'' +
                ", PartitioningKeyFieldName='" + PartitioningKeyFieldName + '\'' +
                ", FieldSchema=" + FieldSchema +
                ", FieldSchemaVersion=" + FieldSchemaVersion +
                '}';
    }
}
