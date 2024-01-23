package io.inlined.gateway;

/** Config key strings. */
public class IKVConstants {
  public static final String S3_BASE_INDEX_BUCKET = "ikv-base-index-v1";

  /** IKVStoreConfig keys */
  public static final String ACCOUNT_ID = "account_id";

  public static final String ACCOUNT_PASSKEY = "account_passkey";

  public static final String MOUNT_DIRECTORY = "mount_directory";

  public static final String STORE_NAME = "store_name";
  public static final String PARTITION = "partition";

  public static final String PRIMARY_KEY_FIELD_NAME = "primary_key_field_name";
  public static final String PARTITIONING_KEY_FIELD_NAME = "partitioning_key_field_name";

  public static final String NUM_KAFKA_PARTITIONS = "num_kafka_partitions";
  public static final String KAFKA_CONSUMER_BOOTSTRAP_SERVER = "kafka_bootstrap_server";
  public static final String KAFKA_CONSUMER_TOPIC_NAME = "kafka_topic";

  public static final String BASE_INDEX_S3_BUCKET_NAME = "base_index_s3_bucket_name";

  public static final String RUST_CLIENT_LOG_LEVEL = "rust_client_log_level";
  public static final String RUST_CLIENT_LOG_TO_CONSOLE = "rust_client_log_to_console";
  public static final String RUST_CLIENT_LOG_FILE = "rust_client_log_file";
}
