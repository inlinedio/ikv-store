package io.inline.gateway;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.inlineio.schemas.Services;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ExtractorUtils {
    public static String extractPrimaryKeyAsString(UserStoreContext context, Map<String, Services.FieldValue> fieldsMap) throws IllegalArgumentException {
        Services.FieldValue primaryKeyFieldValue = fieldsMap.get(context.primaryKeyFieldName());
        Preconditions.checkArgument(primaryKeyFieldValue != null, "primaryKey missing");
        return stringify(primaryKeyFieldValue);
    }

    public static String extractPartitioningKeyAsString(UserStoreContext context, Map<String, Services.FieldValue> fieldsMap) throws IllegalArgumentException {
        Services.FieldValue partitioningKeyFieldValue = fieldsMap.get(context.partitioningKeyFieldName());
        Preconditions.checkArgument(partitioningKeyFieldValue != null, "partitioningKey missing");
        return stringify(partitioningKeyFieldValue);
    }

    private static String stringify(Services.FieldValue fieldValue) {
        switch (fieldValue.getValueCase()) {
            case VALUE_NOT_SET ->
                    throw new IllegalArgumentException("primaryKey missing");
            case STRINGVALUE -> {
                return fieldValue.getStringValue();
            }
            case BYTESVALUE -> {
                ByteString bytes = fieldValue.getBytesValue();
                return bytes.toString(StandardCharsets.UTF_8);
            }
            default -> throw new IllegalArgumentException("wrong data type of primary key");
        }
    }
}
