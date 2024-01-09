package io.inline;

import io.inline.clients.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NearlineIntegrationTests {
  private final ClientOptions _clientOptions =
      new ClientOptions.Builder()
          .withMountDirectory("/tmp/NearlineIntegrationTests")
          .withStoreName("testing-store")
          .withStorePartition(0)
          .withAccountId("testing-account-v1")
          .withAccountPassKey("testing-account-passkey")
              .useStringPrimaryKey("userid")
          .build();

  // kafka topic name - testing-kafka-topic


  @Test
  public void upsertAndRead() throws InterruptedException {
    IKVClientFactory factory = new IKVClientFactory(_clientOptions);
    InlineKVWriter writer = factory.createNewWriterInstance();

    writer.startupWriter();


    IKVDocument document =
        new IKVDocument.Builder().putStringField("userid", "firstuserid").build();
    writer.upsertFieldValues(document);

    Thread.sleep(1000);
  }
}
