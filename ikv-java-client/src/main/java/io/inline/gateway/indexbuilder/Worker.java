package io.inline.gateway.indexbuilder;

import com.google.protobuf.InvalidProtocolBufferException;
import com.inlineio.schemas.Common.*;
import io.inline.clients.internal.IKVClientJNI;
import io.inline.gateway.IKVConstants;
import io.inline.gateway.UserStoreContext;
import io.inline.gateway.ddb.IKVStoreContextController;
import io.inline.gateway.ddb.beans.IKVStoreContext;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// TODO: bug review?
public class Worker {
  private static final Logger LOGGER = LogManager.getLogger(Worker.class);
  private static final String WORKING_DIR =
      String.format("/tmp/ikv-index-builds/%d", Instant.now().toEpochMilli());

  private final IKVStoreContextController _controller;

  public Worker(IKVStoreContextController dynamoDBAccessor) {
    _controller = Objects.requireNonNull(dynamoDBAccessor);
  }

  // Build for all stores.
  public void build(String accountId, String storeName)
      throws InvalidProtocolBufferException, IOException {
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
    IKVStoreConfig sotConfigs = context.createGatewaySpecifiedConfigs();

    String mountDirectory = String.format("%s/%s", WORKING_DIR, accountId);

    // Set some overrides
    IKVStoreConfig config =
        IKVStoreConfig.newBuilder()
            .mergeFrom(sotConfigs)
            .putStringConfigs(IKVConstants.MOUNT_DIRECTORY, mountDirectory)
            .putIntConfigs(IKVConstants.PARTITION, 0) // todo! change - invoke for all partitions.
            .build();

    LOGGER.info(
        "Starting offline build for accountid: {} storename: {} config: {}",
        accountId,
        storeName,
        config);

    Instant start = Instant.now();
    try {
      IKVClientJNI.buildIndex(config.toByteArray());
      LOGGER.info(
          "Successfully finished offline build for accountid: {} storename: {} time: {}s",
          accountId,
          storeName,
          Duration.between(start, Instant.now()).toSeconds());
    } catch (Exception e) {
      LOGGER.error(
          "Error during offline build for accountid: {} storename: {} time: {}s. Error: ",
          accountId,
          storeName,
          Duration.between(start, Instant.now()).toSeconds(),
          e);
    } finally {
      LOGGER.info("Deleting working directory: {}", mountDirectory);
      deleteDirectory(mountDirectory);
    }
  }

  private void deleteDirectory(String directoryPath) throws IOException {
    // https://stackoverflow.com/a/27917071
    Path directory = Paths.get(directoryPath);
    Files.walkFileTree(
        directory,
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            Files.delete(file);
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            Files.delete(dir);
            return FileVisitResult.CONTINUE;
          }
        });
  }
}
