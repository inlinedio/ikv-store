package io.inline.gateway;

import com.google.common.base.Preconditions;
import com.inlineio.schemas.Common.*;
import java.util.Map;

public class ExtractorUtils {
  public static FieldValue extractPrimaryKeyAsString(
      UserStoreContext context, Map<String, FieldValue> fieldsMap) throws IllegalArgumentException {
    FieldValue value = fieldsMap.get(context.primaryKeyFieldName());
    Preconditions.checkArgument(value != null, "primaryKey missing");
    return value;
  }

  public static FieldValue extractPartitioningKeyValue(
      UserStoreContext context, Map<String, FieldValue> fieldsMap) throws IllegalArgumentException {
    FieldValue value = fieldsMap.get(context.partitioningKeyFieldName());
    Preconditions.checkArgument(value != null, "partitioningKey missing");
    return value;
  }
}
