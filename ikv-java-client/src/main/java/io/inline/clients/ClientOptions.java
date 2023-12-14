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

        public static final String CFG_MOUNT_DIRECTORY = "mount_directory";
        public static final String CFG_STORE_NAME = "store_name";
        public static final String CFG_ACCOUNT_ID = "account_id";
        public static final String CFG_ACCOUNT_PASSKEY = "account_passkey";

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

        public Builder withStoreName(String storeName) {
            Preconditions.checkArgument(storeName != null && !storeName.isEmpty());
            _configBuilder.putStringConfigs(CFG_STORE_NAME, storeName);
            return this;
        }

        public Builder withAccountId(String accountId) {
            Preconditions.checkArgument(accountId != null && !accountId.isEmpty());
            _configBuilder.putStringConfigs(CFG_ACCOUNT_ID, accountId);
            return this;
        }

        public Builder withAccountPassKey(String accountPassKey) {
            Preconditions.checkArgument(accountPassKey != null && !accountPassKey.isEmpty());
            _configBuilder.putStringConfigs(CFG_ACCOUNT_PASSKEY, accountPassKey);
            return this;
        }

        public Builder withNumericOverride(String key, int value) {
            _configBuilder.putNumericConfigs(key, value);
            return this;
        }
    }
}
