package io.inline.gateway.ddb;

import java.net.URI;
import javax.annotation.Nullable;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/** Singleton dynamodb client. */
public class DynamoDBEnhancedClientFactory {
  @Nullable private static volatile DynamoDbEnhancedClient CLIENT = null;

  public DynamoDBEnhancedClientFactory() {}

  public static synchronized DynamoDbEnhancedClient getClient() {
    if (CLIENT == null) {
      CLIENT =
          // TODO: we need to specify access credentials
          DynamoDbEnhancedClient.builder()
              .dynamoDbClient(
                  DynamoDbClient.builder()
                      .endpointOverride(URI.create("http://localhost:8000"))
                      // .region(Region.US_EAST_1)
                      .credentialsProvider(ProfileCredentialsProvider.create())
                      .build())
              .build();
    }

    return CLIENT;
  }
}
