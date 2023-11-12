package io.inline.gateway;

import com.google.common.base.Preconditions;
import io.grpc.ServerBuilder;
import io.inline.gateway.streaming.IKVWritesPublisher;

import java.io.IOException;

public class GatewayServer {
    private static final int DEFAULT_PORT = 8080;
    private volatile io.grpc.Server _server;

    public GatewayServer() {
    }

    public void startup() {
        IKVWritesPublisher publisher = new IKVWritesPublisher();

        // start grpc service
        try {
            int port = port();
            _server = ServerBuilder.forPort(port)
                    .addService(new InlineKVWriteServiceImpl(publisher))
                    .build()
                    .start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void shutdown() {
    }

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

}
