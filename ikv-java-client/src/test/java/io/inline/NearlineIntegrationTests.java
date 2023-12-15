package io.inline;

import com.inlineio.schemas.Common;
import io.inline.clients.*;
import org.junit.jupiter.api.Assertions;

public class NearlineIntegrationTests {
  private static final FieldAccessor USERID_ACCESSOR =
      new FieldAccessor("userid", Common.FieldType.STRING);

  private final ClientOptions _clientOptions =
      new ClientOptions.Builder()
          .withMountDirectory("/tmp/NearlineIntegrationTests")
          .withStoreName("testing-store")
          .withAccountId("testing-account-v1")
          .withAccountPassKey("testing-account-passkey")
          .withNumericOverride("kafka_partition", 0) // TODO - remove
          .build();

  private final GRPCInlineKVWriter _writer = new GRPCInlineKVWriter(_clientOptions);
  private final InlineKVReader _reader = new DefaultInlineKVReader();

  // @Test
  public void upsertAndRead() throws InterruptedException {
    _writer.startup();
    _reader.startup(_clientOptions);

    IKVDocument document =
        new IKVDocument.Builder().putStringField("userid", "firstuserid").build();
    _writer.upsertFieldValues(document);

    Thread.sleep(1000 * 10); // 5 sec sleep

    // Read

    String value = _reader.getStringValue(PrimaryKey.from("firstuserid"), USERID_ACCESSOR);
    Assertions.assertEquals(value, "firstuserid");
  }
}
