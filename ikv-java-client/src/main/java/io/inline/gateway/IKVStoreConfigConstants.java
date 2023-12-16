package io.inline.gateway;

/** Config key strings. */
public class IKVStoreConfigConstants {
  public static final String MOUNT_DIRECTORY = "mount_directory";
  public static final String STORE_NAME = "store_name";
  public static final String PARTITION = "partition";
  public static final String PRIMARY_KEY_FIELD_NAME = "primary_key_field_name";
  public static final String PARTITIONING_KEY_FIELD_NAME = "partitioning_key_field_name";
  public static final String NUM_KAFKA_PARTITIONS = "num_kafka_partitions";
  public static final String KAFKA_CONSUMER_BOOTSTRAP_SERVER = "kafka_bootstrap_server";
  public static final String KAFKA_CONSUMER_TOPIC_NAME = "kafka_topic";
  public static final String BASE_INDEX_S3_BUCKET_NAME = "s3_bucket_name";
  public static final String BASE_INDEX_S3_BUCKET_PREFIX = "base_index_s3_bucket_prefix";
}
