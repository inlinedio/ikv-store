package io.inline.gateway;

public class UserStoreContext {
    public static UserStoreContext getDefault() {throw new UnsupportedOperationException();}
    public String kafkaTopicName() {throw new UnsupportedOperationException();}
    public String getStoreName() {
        throw new UnsupportedOperationException();
    }

    public String primaryKeyFieldName() {
        throw new UnsupportedOperationException();
    }

    public String partitioningKeyFieldName() {
        throw new UnsupportedOperationException();
    }

    public int numPartitions() {
        throw new UnsupportedOperationException();
    }
}
