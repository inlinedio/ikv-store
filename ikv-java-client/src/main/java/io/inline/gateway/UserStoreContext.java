package io.inline.gateway;

import com.google.protobuf.InvalidProtocolBufferException;
import com.inlineio.schemas.Common.FieldSchema;
import io.inline.gateway.ddb.beans.IKVStoreContext;

import javax.annotation.Nullable;
import java.util.*;

public class UserStoreContext {
    private final IKVStoreContext _ikvStoreContext;
    private final HashMap<String, FieldSchema> _schema;

    private UserStoreContext(IKVStoreContext ikvStoreContext, HashMap<String, FieldSchema> schema) {
        _ikvStoreContext = Objects.requireNonNull(ikvStoreContext);
        _schema = Objects.requireNonNull(schema);
    }

    public static UserStoreContext from(IKVStoreContext ikvStoreContext) throws InvalidProtocolBufferException {
        HashMap<String, FieldSchema> schema = new HashMap<>();

        // deserialize schema
        List<byte[]> serializedSchema = ikvStoreContext.getFieldSchema();
        for (byte[] bytes : serializedSchema) {
            FieldSchema fieldSchema = FieldSchema.parseFrom(bytes);
            schema.put(fieldSchema.getName(), fieldSchema);
        }

        return new UserStoreContext(ikvStoreContext, schema);
    }

    public String kafkaTopic() {
        return _ikvStoreContext.getKafkaTopicName();
    }

    public int numKafkaPartitions() {return _ikvStoreContext.getNumPartitions();}

    public String primaryKeyFieldName() {
        // TODO: null handling for all field getters defined on _ikvStoreContext
        return _ikvStoreContext.getPrimaryKeyFieldName();
    }

    public String partitioningKeyFieldName() {
        return _ikvStoreContext.getPartitioningKeyFieldName();
    }

    /**
     * Fetch schema for field given it's name.
     * Returns empty for unknown/unregistered fields.
     */
    public Optional<FieldSchema> fieldSchema(String fieldName) {
        @Nullable FieldSchema schema = _schema.getOrDefault(fieldName, null);
        return Optional.ofNullable(schema);
    }
}
