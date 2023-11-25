package io.inline.clients;

import com.google.common.base.Preconditions;
import com.inlineio.schemas.Common.IKVStoreConfig;

import java.util.Objects;

public final class ClientOptions {
    private final IKVStoreConfig _config;

    private ClientOptions(IKVStoreConfig config) {
        _config = Objects.requireNonNull(config);
    }

    IKVStoreConfig config() {
        return _config;
    }

    public final static class Builder {
        private final IKVStoreConfig.Builder _configBuilder;

        private static final String CFG_MOUNT_DIRECTORY = "mount_directory";
        private static final String CFG_PRIMARY_KEY = "primary_key";
        private static final String CFG_STORE_NAME = "store_name";

        private static final String CFG_KAFKA_BOOTSTRAP_SERVER = "kafka_consumer_bootstrap_server";  // comes from inline cloud
        private static final String CFG_KAFKA_TOPIC = "kafka_topic"; // comes from inline cloud
        private static final String CFG_KAFKA_PARTITION = "kafka_partition"; // comes from inline cloud

        public Builder() {
            _configBuilder = IKVStoreConfig.newBuilder();
        }

        public ClientOptions build() {
            return new ClientOptions(_configBuilder.build());
        }

        public Builder withMountDirectory(String mountDirectory) {
            Preconditions.checkArgument(mountDirectory != null && !mountDirectory.isEmpty());
            _configBuilder.putStringConfigs(CFG_MOUNT_DIRECTORY, mountDirectory);
            return this;
        }

        public Builder withPrimaryKeyFieldName(String primaryKeyFieldName) {
            Preconditions.checkArgument(primaryKeyFieldName != null && !primaryKeyFieldName.isEmpty());
            _configBuilder.putStringConfigs(CFG_PRIMARY_KEY, primaryKeyFieldName);
            return this;
        }

        public Builder withStoreName(String storeName) {
            Preconditions.checkArgument(storeName != null && !storeName.isEmpty());
            _configBuilder.putStringConfigs(CFG_STORE_NAME, storeName);
            return this;
        }

        @Deprecated
        public Builder withKafkaConsumerBootstrapServer(String bootstrapServer) {
            Preconditions.checkArgument(bootstrapServer != null && !bootstrapServer.isEmpty());
            _configBuilder.putStringConfigs(CFG_KAFKA_BOOTSTRAP_SERVER, bootstrapServer);
            return this;
        }

        @Deprecated
        public Builder withKafkaConsumerTopic(String topic) {
            Preconditions.checkArgument(topic != null && !topic.isEmpty());
            _configBuilder.putStringConfigs(CFG_KAFKA_TOPIC, topic);
            return this;
        }

        @Deprecated
        public Builder withKafkaConsumerPartition(int partition) {
            Preconditions.checkArgument(partition >= 0);
            _configBuilder.putNumericConfigs(CFG_KAFKA_PARTITION, partition);
            return this;
        }
    }
}
