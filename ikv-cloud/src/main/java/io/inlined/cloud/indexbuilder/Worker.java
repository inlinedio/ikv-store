package io.inlined.cloud.indexbuilder;

import com.google.common.base.Preconditions;
import com.inlineio.schemas.Common.*;
import io.inlined.clients.IKVClientJNI;
import io.inlined.clients.IKVConstants;
import io.inlined.cloud.UserStoreContext;
import io.inlined.cloud.ddb.IKVStoreContextObjectsAccessor;
import io.inlined.cloud.ddb.IKVStoreContextObjectsAccessorFactory;
import io.inlined.cloud.ddb.beans.IKVStoreContext;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Worker {
  private static final Logger LOGGER = LogManager.getLogger(Worker.class);
  private static final String WORKING_DIR = "/tmp/ikv-index-builds";

  private final IKVStoreContextObjectsAccessor _controller;

  /**
   * Usage - java -cp /path/to/jar io.inlined.cloud.indexbuilder.Worker {account-id} {store-name}
   */
  public static void main(String[] args) throws IOException {
    IKVStoreContextObjectsAccessor accessor = IKVStoreContextObjectsAccessorFactory.getAccessor();
    Worker worker = new Worker(accessor);

    String accountId = args[0];
    String storeName = args[1];

    worker.build(accountId, storeName, 0); // partition=0 hardcoded for now
  }

  public Worker(IKVStoreContextObjectsAccessor dynamoDBAccessor) {
    _controller = Objects.requireNonNull(dynamoDBAccessor);
  }

  // Build for all stores.
  public void build(String accountId, String storeName, int partition) throws IOException {
    // pass in the mount point for native binary
    // can be different than mount point for actual DB
    IKVClientJNI ikvClientJNI = IKVClientJNI.createNew(WORKING_DIR);
    Preconditions.checkNotNull(ikvClientJNI.provideHelloWorld(), "Linkage error");

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

    // mount dir MUST be different per run - to foce a base index download
    // from S3. Else left over data can be used
    String mountDirectory =
        String.format("%s/%d/%s", WORKING_DIR, Instant.now().toEpochMilli(), accountId);
    deleteDirectory(mountDirectory); // cleanup

    // Set some overrides
    IKVStoreConfig config =
        IKVStoreConfig.newBuilder()
            .mergeFrom(sotConfigs)
            .putStringConfigs(IKVConstants.ACCOUNT_PASSKEY, context.accountPasskey())
            .putStringConfigs(IKVConstants.MOUNT_DIRECTORY, mountDirectory)
            .putStringConfigs(IKVConstants.RUST_CLIENT_LOG_LEVEL, "info")
            .putBooleanConfigs(IKVConstants.RUST_CLIENT_LOG_TO_CONSOLE, true)
            .putIntConfigs(IKVConstants.PARTITION, partition)
            .build();

    // Never print configs since it has passkeys
    LOGGER.info(
        "Starting index build for accountid: {} storename: {}, mount-dir: {}",
        accountId,
        storeName,
        mountDirectory);

    Instant start = Instant.now();
    try {
      ikvClientJNI.buildIndex(config.toByteArray());
      LOGGER.info(
          "Successfully finished offline build for accountid: {} storename: {} time: {}s",
          accountId,
          storeName,
          Duration.between(start, Instant.now()).toSeconds());

      LOGGER.info("Deleting mount directory: {}", mountDirectory);
      deleteDirectory(mountDirectory);

    } catch (Exception e) {
      LOGGER.error(
          "Error during offline build for accountid: {} storename: {} time: {}s. Error: ",
          accountId,
          storeName,
          Duration.between(start, Instant.now()).toSeconds(),
          e);
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
