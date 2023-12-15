package io.inline.gateway.indexbuilder;

import com.google.protobuf.InvalidProtocolBufferException;
import com.inlineio.schemas.Common.*;
import io.inline.clients.internal.IKVClientJNI;
import io.inline.gateway.IKVStoreConfigConstants;
import io.inline.gateway.UserStoreContext;
import io.inline.gateway.ddb.IKVStoreContextController;
import io.inline.gateway.ddb.beans.IKVStoreContext;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Worker {
  private static final Logger LOGGER = LogManager.getLogger(Worker.class);
  private static final String WORKING_DIR = "/tmp/ikvindexes/";

  private final IKVStoreContextController _controller;

  public Worker(IKVStoreContextController dynamoDBAccessor) {
    _controller = Objects.requireNonNull(dynamoDBAccessor);
  }

  // Build for all stores.
  public void build(String accountId, String storeName) throws InvalidProtocolBufferException {
    Optional<IKVStoreContext> maybeContext = _controller.getItem(accountId, storeName);
    if (maybeContext.isEmpty()) {
      // Invalid args
      LOGGER.error(
          "Invalid store args for offline index build, " + "accountid: {} storename: {}",
          accountId,
          storeName);
      return;
    }

    // Build configs
    UserStoreContext context = UserStoreContext.from(maybeContext.get());
    IKVStoreConfig sotConfigs = context.createConfig();

    // Set some overrides. Mount directory
    String mountDirectory = mountDirectory(accountId, storeName);

    IKVStoreConfig config =
        IKVStoreConfig.newBuilder()
            .mergeFrom(sotConfigs)
            .putStringConfigs(IKVStoreConfigConstants.MOUNT_DIRECTORY, mountDirectory)
            .build();

    LOGGER.info(
        "Starting offline build for accountid: {} storename: {} config: {}",
        accountId,
        storeName,
        config);

    try {
      IKVClientJNI.buildIndex(config.toByteArray());
      LOGGER.info(
          "Successfully finished offline build for accountid: {} storename: {}",
          accountId,
          storeName);
    } catch (Exception e) {
      LOGGER.error(
          "Error during offline build for accountid: {} storename: {}. Error: ",
          accountId,
          storeName,
          e);
      return;
    }

    // Todo: upload mount directory to S3.
  }

  private static String mountDirectory(String accountId, String storeName) {
    // mount dir: WORKING_DIR/<accountid>/<storename>/<epoch>/partition/
    long currentEpochMillis = Instant.now().toEpochMilli();
    return String.format(
        "%s/%s/%s/%d/%d", WORKING_DIR, accountId, storeName, currentEpochMillis, 0);
  }
}
