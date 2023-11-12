package io.inline.gateway;

// Startup class for gateway service
public class Main {
    public static void main(String[] args) throws InterruptedException {
        GatewayServer server = new GatewayServer();
        server.startup();
        server.blockUntilShutdown();
    }
}