package io.inlined.clients;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

/**
 * Remote s3 key format:
 * release/{mac|linux|windows}-{aarch64|x86_64|tbd}/0.0.1-{libikv.dylib|libikv.so|tbd}
 *
 * <p>Ex- release/mac-aarch64/0.0.1-libikv.dylib release/linux-aarch64/0.0.1-libikv.so
 * release/linux-x86_64/0.0.1-libikv.so
 */
public class NativeBinaryManager {
  private static final Logger LOGGER = LogManager.getLogger(NativeBinaryManager.class);
  private final S3Client _s3Client;
  private final String _mountDirectory;

  public NativeBinaryManager(String mountDirectory) {
    _s3Client = S3Client.builder().region(Region.US_WEST_2).build();
    _mountDirectory = mountDirectory;
  }

  public void close() {
    _s3Client.close();
  }

  public Optional<String> getPathToNativeBinary() throws IOException {
    Optional<String> maybeCurrentSemVer = currentSemVer();
    Optional<String> maybeHighestAvailableSemVer = highestAvailableSemVer();

    if (maybeHighestAvailableSemVer.isEmpty()) {
      // nothing available on remote, use local native binary (can be empty)
      return pathToCurrentNativeBinary();
    }

    String highestAvailableSemVer = maybeHighestAvailableSemVer.get();
    if (maybeCurrentSemVer.isEmpty()
        || compareSemVer(highestAvailableSemVer, maybeCurrentSemVer.get()) > 0) {
      clearLocalNativeBinaries();
      downloadHighestAvailableSemVerNativeBinary();
    }

    return pathToCurrentNativeBinary();
  }

  /** Existing sem-ver of the local native-binary. */
  private Optional<String> currentSemVer() {
    Optional<String> maybePathToCurrentNativeBinary = pathToCurrentNativeBinary();
    if (maybePathToCurrentNativeBinary.isEmpty()) {
      return Optional.empty();
    }

    String[] parts = maybePathToCurrentNativeBinary.get().split("/");
    String filename = parts[parts.length - 1]; // 0.0.1-libikv.dylib

    parts = filename.split("-");
    return Optional.of(parts[0]);
  }

  private Optional<String> pathToCurrentNativeBinary() {
    // Check if <mount_dir>/bin exists
    String dirPath = String.format("%s/bin", _mountDirectory);
    if (!Files.exists(Paths.get(dirPath))) {
      return Optional.empty();
    }

    // format of files: 0.0.1-libikv.dylib
    List<String> filenames =
        Stream.of(new File(dirPath).listFiles())
            .filter(file -> !file.isDirectory())
            .map(File::getName)
            .toList();

    if (filenames.isEmpty()) {
      return Optional.empty();
    }

    // Assume there is only one file
    return Optional.of(String.format("%s/%s", dirPath, filenames.get(0)));
  }

  /**
   * Highest available sem-ver of the remote native-binary. Note that this version might expire
   * before actual download.
   */
  private Optional<String> highestAvailableSemVer() {
    Optional<S3Object> maybeS3Object = highestAvailableSemVerS3Object();
    if (maybeS3Object.isEmpty()) {
      return Optional.empty();
    }

    // Example- release/mac-aarch64/0.0.1-libikv.dylib
    String key = maybeS3Object.get().key();

    String[] parts = key.split("/");
    String semVer = parts[2].split("-")[0];

    return Optional.of(semVer);
  }

  private Optional<S3Object> highestAvailableSemVerS3Object() {
    Optional<String> maybePrefix = createS3PrefixForPlatform();
    if (maybePrefix.isEmpty()) {
      return Optional.empty();
    }

    ListObjectsV2Request listObjectsRequest =
        ListObjectsV2Request.builder().bucket("ikv-binaries").prefix(maybePrefix.get()).build();
    ListObjectsV2Response listObjectsResponse = _s3Client.listObjectsV2(listObjectsRequest);
    return maxSemVerS3Object(listObjectsResponse.contents());
  }

  private void clearLocalNativeBinaries() {
    // Check if <mount_dir>/bin exists
    String dirPath = String.format("%s/bin", _mountDirectory);
    if (!Files.exists(Paths.get(dirPath))) {
      return;
    }

    File directory = new File(dirPath);
    if (directory.exists() && directory.isDirectory()) {
      File[] files = directory.listFiles();
      for (File file : files) {
        Preconditions.checkArgument(file.delete(), "Could not delete: %s", file.getAbsolutePath());
      }
    }
  }

  private void downloadHighestAvailableSemVerNativeBinary() throws IOException {
    Optional<S3Object> s3Object = highestAvailableSemVerS3Object();
    if (s3Object.isEmpty()) {
      return;
    }

    // Ensure <mount_dir>/bin exists
    // This method does not throw an exception if the directory already exists.
    String dirPath = String.format("%s/bin", _mountDirectory);
    Files.createDirectories(Paths.get(dirPath));

    try {
      // TODO: it is possible that the key is already stale in S3.
      String key = s3Object.get().key(); // ex. release/mac-aarch64/0.0.1-libikv.dylib
      GetObjectRequest getObjectRequest =
          GetObjectRequest.builder().bucket("ikv-binaries").key(key).build();
      String filename = String.format("%s/%s", dirPath, key.split("/")[2]);

      // download and write to ex. - "<mount_directory>/bin/0.0.1-libikv.dylib"
      ResponseBytes<GetObjectResponse> objectBytes = _s3Client.getObjectAsBytes(getObjectRequest);
      byte[] contentBytes = objectBytes.asByteArray();
      try (FileOutputStream fos = new FileOutputStream(filename)) {
        fos.write(contentBytes);
      }
    } catch (FileNotFoundException e) {
      // thrown if filepath was dir, cannot be opened etc.
      throw new IOException(e);
    } catch (S3Exception e) {
      throw new IOException(e);
    }
  }

  // release/{mac|linux|windows}-{aarch64|x86_64|tbd}
  private static Optional<String> createS3PrefixForPlatform() throws RuntimeException {
    String osType;
    String osName = Objects.requireNonNull(System.getProperty("os.name")).toLowerCase();
    if (osName.contains("linux")) {
      osType = "linux";
    } else if (osName.contains("mac")) {
      osType = "mac";
    } else if (osName.contains("window")) {
      LOGGER.error("Windows support not available at the moment");
      return Optional.empty();
    } else {
      LOGGER.error("Unsupported Operating System: {}", osName);
      return Optional.empty();
    }

    String osArchitecture;
    String osArch = Objects.requireNonNull(System.getProperty("os.arch")).toLowerCase();
    if (osArch.contains("aarch64")) {
      osArchitecture = "aarch64";
    } else if (osArch.contains("x86_64") || osArch.contains("amd64")) {
      osArchitecture = "x86_64";
    } else {
      LOGGER.error("Unsupported Architecture: {}", osArch);
      return Optional.empty();
    }

    LOGGER.info("Local os-type: {} architecture: {}", osType, osArchitecture);
    return Optional.of(String.format("release/%s-%s", osType, osArchitecture));
  }

  private static Optional<S3Object> maxSemVerS3Object(List<S3Object> objects) {
    if (objects.size() == 0) {
      return Optional.empty();
    }

    String highestSemVer = null;
    S3Object objectWithHighestSemVer = null;

    // loop to filter out invalid keys and find the one with highest SemVer (x.y.z)
    for (S3Object s3Object : objects) {
      Optional<String> maybeSemVer = getOptionalSemVersion(s3Object.key());
      if (maybeSemVer.isPresent()) {
        String semVer = maybeSemVer.get();
        if (highestSemVer == null || compareSemVer(semVer, highestSemVer) > 0) {
          highestSemVer = semVer;
          objectWithHighestSemVer = s3Object;
        }
      }
    }

    return Optional.ofNullable(objectWithHighestSemVer);
  }

  private static Optional<String> getOptionalSemVersion(String s3ObjectKey) {
    // release/linux-x86_64/0.0.1-libikv.so
    String[] parts = s3ObjectKey.split("/");
    if (parts.length != 3) {
      return Optional.empty();
    }

    String version = parts[2].split("-")[0];
    if (version.matches("\\d+\\.\\d+\\.\\d+")) {
      return Optional.of(version);
    }

    return Optional.empty();
  }

  private static int compareSemVer(String version1, String version2) {
    String[] parts1 = version1.split("\\.");
    String[] parts2 = version2.split("\\.");

    for (int i = 0; i < 3; i++) {
      int part1 = Integer.parseInt(parts1[i]);
      int part2 = Integer.parseInt(parts2[i]);

      if (part1 != part2) {
        return Integer.compare(part1, part2);
      }
    }

    return 0;
  }
}
