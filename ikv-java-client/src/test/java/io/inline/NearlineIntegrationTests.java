package io.inline;

import io.inline.clients.*;
import org.junit.jupiter.api.Assertions;

public class NearlineIntegrationTests {
  private static final String USERID_ACCESSOR = "userid";

  private final ClientOptions _clientOptions =
      new ClientOptions.Builder()
          .withMountDirectory("/tmp/NearlineIntegrationTests")
          .withStoreName("testing-store")
          .withStorePartition(0)
          .withAccountId("testing-account-v1")
          .withAccountPassKey("testing-account-passkey")
          .build();

  private final DefaultInlineKVWriter _writer = new DefaultInlineKVWriter(_clientOptions);
  private final InlineKVReader _reader = new DefaultInlineKVReader(_clientOptions);

  // @Test
  public void upsertAndRead() throws InterruptedException {
    _writer.startupWriter();
    _reader.startupReader();

    IKVDocument document =
        new IKVDocument.Builder().putStringField("userid", "firstuserid").build();
    _writer.upsertFieldValues(document);

    Thread.sleep(1000 * 10); // 5 sec sleep

    // Read

    String value = _reader.getStringValue("firstuserid", USERID_ACCESSOR);
    Assertions.assertEquals(value, "firstuserid");
  }
}
