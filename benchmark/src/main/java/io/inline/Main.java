package io.inline;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hello world!");
        DynamoDbClient client = DynamoDbClient.builder()
                .endpointOverride(URI.create("http://localhost:8000"))
                // The region is meaningless for local DynamoDb but required for client builder validation
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("dummy-key", "dummy-secret")))
                .build();
    }
}