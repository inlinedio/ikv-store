package io.inline.gateway;

import com.inlineio.schemas.Common;

import java.util.Optional;

public class UserStoreContext {
    public static UserStoreContext getDefault() {throw new UnsupportedOperationException();}
    public String kafkaTopicName() {throw new UnsupportedOperationException();}
    public String storeName() {
        throw new UnsupportedOperationException();
    }

    public String primaryKey() {
        throw new UnsupportedOperationException();
    }

    public String partitioningKey() {
        throw new UnsupportedOperationException();
    }

    public int numPartitions() {
        throw new UnsupportedOperationException();
    }

    public Optional<Common.FieldSchema> fieldSchema(String fieldName) {
        // serve from local cache or fetch remotely for new fields
        throw new UnsupportedOperationException();
    }
}
