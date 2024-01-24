package io.inlined.cloud;

import com.google.common.base.Preconditions;
import io.grpc.ServerBuilder;
import io.inlined.cloud.ddb.IKVStoreContextObjectsAccessor;
import io.inlined.cloud.ddb.IKVStoreContextObjectsAccessorFactory;
import io.inlined.cloud.streaming.IKVKafkaWriter;
import io.inlined.cloud.streaming.KafkaProducerFactory;
import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GatewayServer {
  private static final Logger LOGGER = LogManager.getLogger(GatewayServer.class);

  // tip: lsof -n -i :8080
  // Sometimes server can fail to bind to the port
  // Use the above to kill processes.
  private static final int DEFAULT_PORT = 8080;
  private volatile io.grpc.Server _server;

  public static void main(String[] args) throws IOException, InterruptedException {
    LOGGER.info("Hello from log4j");

    GatewayServer server = new GatewayServer();
    server.startup();
    server.blockUntilShutdown();
  }

  public GatewayServer() {}

  public void startup() {
    IKVKafkaWriter publisher = new IKVKafkaWriter(KafkaProducerFactory.createInstance());
    IKVStoreContextObjectsAccessor ikvStoreContextObjectsAccessor =
        IKVStoreContextObjectsAccessorFactory.getAccessor();
    UserStoreContextAccessor userStoreContextAccessor =
        new UserStoreContextAccessor(ikvStoreContextObjectsAccessor);

    // start grpc service
    try {
      int port = port();
      _server =
          ServerBuilder.forPort(port)
              .addService(new InlineKVWriteServiceImpl(publisher, userStoreContextAccessor))
              .build()
              .start();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void shutdown() {}

  public void blockUntilShutdown() throws InterruptedException {
    Preconditions.checkNotNull(_server);
    LOGGER.info("Server is listening!");
    _server.awaitTermination();
  }

  private int port() {
    try {
      return Integer.parseInt(System.getenv("PORT"));
    } catch (NullPointerException e) {
      // Not present
    } catch (NumberFormatException e) {
      // invalid format
    }

    return DEFAULT_PORT;
  }
}
