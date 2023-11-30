package io.inline.gateway;

import com.inlineio.schemas.Common.FieldSchema;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class UserStoreContext {
    private final String _accountName;
    private final String _storeName;

    // document schema
    private final String _primaryKeyFieldName;
    private final String _partitioningKeyFieldName;

    // TODO: dynamodb client to fetch schema on demand!
    private final AtomicInteger _schemaIdGenerator; // current value is unused ID.
    private final ConcurrentHashMap<String, FieldSchema> _schema;

    private final String _kafkaTopic;

    @Deprecated
    // This should be created from DynamoDB object
    public UserStoreContext(String accountName, String storeName,
                            String primaryKeyName, String partitioningKeyName, Map<String, FieldSchema> initialSchema, String kafkaTopic) {
        _accountName = accountName;
        _storeName = storeName;
        _primaryKeyFieldName = primaryKeyName;
        _partitioningKeyFieldName = partitioningKeyName;
        _schemaIdGenerator = new AtomicInteger(0);
        _schema = new ConcurrentHashMap<>(initialSchema);
        _kafkaTopic = kafkaTopic;
    }

    public String kafkaTopic() {
        return _kafkaTopic;
    }

    public String primaryKey() {
        return _primaryKeyFieldName;
    }

    public String partitioningKey() {
        return _partitioningKeyFieldName;
    }

    public Optional<FieldSchema> fieldSchema(String fieldName) {
        @Nullable FieldSchema schema = _schema.getOrDefault(fieldName, null);
        if (schema != null) {
            return Optional.of(schema);
        }

        // Fetch from dynamodb?
        throw new UnsupportedOperationException();
    }

    public void updateSchema(Collection<FieldSchema> fieldsToAdd) throws IllegalArgumentException {
        if (fieldsToAdd == null || fieldsToAdd.isEmpty()) {
            return;
        }

        // Conflict checks
        for (FieldSchema incomingFieldSchema : fieldsToAdd) {
            @Nullable FieldSchema existing = _schema.get(incomingFieldSchema.getName());
            if (existing != null) {
                // field exists, throw if there's a type conflict
                if (incomingFieldSchema.getFieldTypeValue() == 0) {
                    // unknown type, throw
                    throw new IllegalArgumentException(String.format("Field: %s has a new type which is not supported yet, try later.", incomingFieldSchema.getName()));
                }
                if (incomingFieldSchema.getFieldTypeValue() != existing.getFieldTypeValue()) {
                    // type conflict, throw
                    throw new IllegalArgumentException(String.format("Type conflict found for Field: %s Existing: %s Requested: %s",
                            incomingFieldSchema.getName(),
                            existing.getFieldType(),
                            incomingFieldSchema.getFieldType()));
                }
            }
        }

        // Insert
        for (FieldSchema incomingFieldSchema : fieldsToAdd) {
            // TODO: write to DynamoDB
            FieldSchema fieldSchema = FieldSchema.newBuilder()
                    .setName(incomingFieldSchema.getName())
                    .setId(_schemaIdGenerator.getAndIncrement())
                    .setFieldType(incomingFieldSchema.getFieldType())
                    .build();
            _schema.put(fieldSchema.getName(), fieldSchema);
        }
    }
}
