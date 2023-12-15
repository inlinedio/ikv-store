package io.inline.gateway;

import com.google.common.base.Preconditions;
import io.grpc.ServerBuilder;
import io.inline.gateway.ddb.IKVStoreContextController;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;

public class GatewayServer {
  private static final Logger LOGGER = LogManager.getLogger(GatewayServer.class);
  private static final int DEFAULT_PORT = 8081;
  private volatile io.grpc.Server _server;

  public static void main(String[] args) throws IOException, InterruptedException {
    // Setup log4j
    configureLog4j();

    GatewayServer server = new GatewayServer();
    server.startup();
    server.blockUntilShutdown();
  }

  public GatewayServer() {}

  public void startup() {
    LOGGER.info("Starting server!");
    IKVWriter publisher = new IKVWriter();
    IKVStoreContextController ikvStoreContextController = new IKVStoreContextController();
    UserStoreContextAccessor userStoreContextAccessor =
        new UserStoreContextAccessor(ikvStoreContextController);

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
    System.out.println("Server is listening!");
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

  private static void configureLog4j() throws IOException {
    URL url = GatewayServer.class.getClassLoader().getResource("log4j.xml");
    File file;
    try {
      file = new File(url.toURI());
    } catch (NullPointerException e) {
      throw e;
    } catch (URISyntaxException e) {
      file = new File(url.getPath());
    }

    /*
    LoggerContext context = (LoggerContext) LogManager.getContext();
    ConfigurationSource source = new ConfigurationSource(GatewayServer.class.getClassLoader().getResourceAsStream("log4j.xml"));
    ConfigurationFactory factory = new XmlConfigurationFactory();
    Configuration configuration = factory.getConfiguration(context, source);
    context.start(configuration);
    context.updateLoggers();
    */

    LoggerContext context = (LoggerContext) LogManager.getContext(false);
    context.setConfigLocation(file.toURI());
  }
}
