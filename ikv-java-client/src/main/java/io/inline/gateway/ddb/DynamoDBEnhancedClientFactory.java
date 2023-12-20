package io.inline.gateway.ddb;

import javax.annotation.Nullable;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.regions.Region;
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
                      // .endpointOverride(URI.create("http://localhost:8000"))
                      .region(Region.EU_NORTH_1)
                      // use:
                      // https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/ContainerCredentialsProvider.html
                      .credentialsProvider(DefaultCredentialsProvider.builder().build())
                      .build())
              .build();
    }

    return CLIENT;
  }
}
