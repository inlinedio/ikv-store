package io.inline.gateway.ddb.beans;


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
    private String accountId;
    private String storeName;
    private Integer numPartitions;
    private String kafkaTopicName;

    private String primaryKeyFieldName;
    private String partitioningKeyFieldName;

    // List of serialized FieldSchema.proto objects
    private List<byte[]> fieldSchema;
    private Integer fieldSchemaVersion;  // starts from 1

    @DynamoDbPartitionKey
    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }


    @DynamoDbSortKey
    public String getStoreName() {
        return storeName;
    }

    public void setStoreName(String storeName) {
        this.storeName = storeName;
    }

    public Integer getNumPartitions() {
        return numPartitions;
    }

    public void setNumPartitions(Integer numPartitions) {
        this.numPartitions = numPartitions;
    }

    public String getKafkaTopicName() {
        return kafkaTopicName;
    }

    public void setKafkaTopicName(String kafkaTopicName) {
        this.kafkaTopicName = kafkaTopicName;
    }

    public String getPrimaryKeyFieldName() {
        return primaryKeyFieldName;
    }

    public void setPrimaryKeyFieldName(String primaryKeyFieldName) {
        this.primaryKeyFieldName = primaryKeyFieldName;
    }

    public String getPartitioningKeyFieldName() {
        return partitioningKeyFieldName;
    }

    public void setPartitioningKeyFieldName(String partitioningKeyFieldName) {
        this.partitioningKeyFieldName = partitioningKeyFieldName;
    }

    public List<byte[]> getFieldSchema() {
        return fieldSchema;
    }

    public void setFieldSchema(List<byte[]> fieldSchema) {
        this.fieldSchema = fieldSchema;
    }

    public Integer getFieldSchemaVersion() {
        return fieldSchemaVersion;
    }

    public void setFieldSchemaVersion(Integer fieldSchemaVersion) {
        this.fieldSchemaVersion = fieldSchemaVersion;
    }

    @Override
    public String toString() {
        return "IKVStoreContext{" +
                "accountId='" + accountId + '\'' +
                ", storeName='" + storeName + '\'' +
                ", numPartitions=" + numPartitions +
                ", kafkaTopicName='" + kafkaTopicName + '\'' +
                ", primaryKeyFieldName='" + primaryKeyFieldName + '\'' +
                ", partitioningKeyFieldName='" + partitioningKeyFieldName + '\'' +
                ", fieldSchema=" + fieldSchema +
                ", fieldSchemaVersion=" + fieldSchemaVersion +
                '}';
    }
}
