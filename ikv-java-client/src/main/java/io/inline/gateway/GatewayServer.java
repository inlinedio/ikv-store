package io.inline.gateway;

import com.google.common.base.Preconditions;
import io.grpc.ServerBuilder;
import io.inline.gateway.ddb.IKVStoreContextObjectsAccessor;
import io.inline.gateway.ddb.IKVStoreContextObjectsAccessorFactory;
import io.inline.gateway.streaming.IKVKafkaWriter;
import io.inline.gateway.streaming.KafkaProducerFactory;
import java.io.IOException;
import java.io.InputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.xml.XmlConfigurationFactory;

public class GatewayServer {
  private static final Logger LOGGER = LogManager.getLogger(GatewayServer.class);
  private static final int DEFAULT_PORT = 8080;
  private volatile io.grpc.Server _server;

  public static void main(String[] args) throws IOException, InterruptedException {
    configureLog4j();

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

  private static void configureLog4j() throws IOException {
    InputStream in = GatewayServer.class.getClassLoader().getResourceAsStream("log4j.xml");
    Preconditions.checkNotNull(in);
    ConfigurationSource source = new ConfigurationSource(in);
    ConfigurationFactory factory = new XmlConfigurationFactory();
    LoggerContext context = (LoggerContext) LogManager.getContext(false);
    Configuration configuration = factory.getConfiguration(context, source);
    context.start(configuration);
    context.updateLoggers();
  }
}
