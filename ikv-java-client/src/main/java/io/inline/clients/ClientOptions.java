package io.inline.clients;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.inlineio.schemas.Common.IKVStoreConfig;
import com.inlineio.schemas.Services;
import io.inline.gateway.IKVStoreConfigConstants;
import java.util.Objects;
import java.util.Set;

public final class ClientOptions {
  private final IKVStoreConfig _config;

  private ClientOptions(IKVStoreConfig config) {
    _config = Objects.requireNonNull(config);
  }

  public IKVStoreConfig asIKVStoreConfig() {
    return _config;
  }

  public Services.UserStoreContextInitializer createUserStoreContextInitializer() {
    return Services.UserStoreContextInitializer.newBuilder()
        .setStoreName(_config.getStringConfigsOrThrow(IKVStoreConfigConstants.STORE_NAME))
        .setCredentials(
            Services.AccountCredentials.newBuilder()
                .setAccountId(_config.getStringConfigsOrThrow(ClientOptions.Builder.CFG_ACCOUNT_ID))
                .setAccountPasskey(
                    _config.getStringConfigsOrThrow(ClientOptions.Builder.CFG_ACCOUNT_PASSKEY))
                .build())
        .build();
  }

  public static final class Builder {
    private static final String CFG_ACCOUNT_ID = "account_id";
    private static final String CFG_ACCOUNT_PASSKEY = "account_passkey";
    private static final Set<String> LOG_LEVELS =
        ImmutableSet.of("error", "warn", "info", "debug", "trace");

    private final IKVStoreConfig.Builder _configBuilder;

    public Builder() {
      _configBuilder = IKVStoreConfig.newBuilder();

      // defaults
      _configBuilder.putStringConfigs(IKVStoreConfigConstants.RUST_CLIENT_LOG_LEVEL, "info");
      _configBuilder.putBooleanConfigs(IKVStoreConfigConstants.RUST_CLIENT_LOG_TO_CONSOLE, true);
    }

    public ClientOptions build() {
      // enforce required configs
      _configBuilder.getStringConfigsOrThrow(CFG_ACCOUNT_ID);
      _configBuilder.getStringConfigsOrThrow(CFG_ACCOUNT_PASSKEY);
      _configBuilder.getStringConfigsOrThrow(IKVStoreConfigConstants.STORE_NAME);
      _configBuilder.getStringConfigsOrThrow(IKVStoreConfigConstants.MOUNT_DIRECTORY);
      _configBuilder.getNumericConfigsOrThrow(IKVStoreConfigConstants.PARTITION);
      _configBuilder.getStringConfigsOrThrow(IKVStoreConfigConstants.PRIMARY_KEY_FIELD_NAME);

      return new ClientOptions(_configBuilder.build());
    }

    public Builder withMountDirectory(String mountDirectory) {
      // user specified mount path
      Preconditions.checkArgument(mountDirectory != null && !mountDirectory.isEmpty());
      _configBuilder.putStringConfigs(IKVStoreConfigConstants.MOUNT_DIRECTORY, mountDirectory);
      return this;
    }

    public Builder withStoreName(String storeName) {
      Preconditions.checkArgument(storeName != null && !storeName.isEmpty());
      _configBuilder.putStringConfigs(IKVStoreConfigConstants.STORE_NAME, storeName);
      return this;
    }

    public Builder withStorePartition(int partition) {
      Preconditions.checkArgument(partition > 0);
      _configBuilder.putNumericConfigs(IKVStoreConfigConstants.PARTITION, partition);
      _configBuilder.putNumericConfigs(IKVStoreConfigConstants.KAFKA_CONSUMER_PARTITION, partition);
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

    public Builder withConsoleLogging(String level) {
      Preconditions.checkArgument(level != null && !level.isEmpty());
      Preconditions.checkArgument(
          LOG_LEVELS.contains(level.toLowerCase()), "log-level should be one of: {}", LOG_LEVELS);
      _configBuilder.putStringConfigs(
          IKVStoreConfigConstants.RUST_CLIENT_LOG_LEVEL, level.toLowerCase());
      _configBuilder.putBooleanConfigs(IKVStoreConfigConstants.RUST_CLIENT_LOG_TO_CONSOLE, true);
      return this;
    }

    public Builder withFileLogging(String filePath, String level) {
      Preconditions.checkArgument(filePath != null && !filePath.isEmpty());
      Preconditions.checkArgument(level != null && !level.isEmpty());
      Preconditions.checkArgument(
          LOG_LEVELS.contains(level.toLowerCase()), "log-level should be one of: {}", LOG_LEVELS);
      _configBuilder.putStringConfigs(
          IKVStoreConfigConstants.RUST_CLIENT_LOG_LEVEL, level.toLowerCase());
      _configBuilder.putBooleanConfigs(IKVStoreConfigConstants.RUST_CLIENT_LOG_TO_CONSOLE, false);
      _configBuilder.putStringConfigs(IKVStoreConfigConstants.RUST_CLIENT_LOG_FILE, filePath);
      return this;
    }
  }
}
