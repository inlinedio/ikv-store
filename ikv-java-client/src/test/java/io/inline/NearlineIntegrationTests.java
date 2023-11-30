package io.inline;

import com.inlineio.schemas.Common;
import io.inline.clients.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NearlineIntegrationTests {
    private final static FieldAccessor USERID_ACCESSOR =
            new FieldAccessor("userid", Common.FieldType.BYTES);

    private final ClientOptions _clientOptions = new ClientOptions.Builder()
            .withMountDirectory("/tmp/NearlineIntegrationTests")
            .withPrimaryKeyFieldName("userid")
            .withStoreName("testing-store")
            .withKafkaConsumerBootstrapServer("localhost:9092")
            .withKafkaConsumerTopic("testing-topic")
            .withKafkaConsumerPartition(0)
            .build();

    private InlineKVWriter _writer = new GRPCInlineKVWriter();
    private InlineKVReader _reader = new DefaultInlineKVReader();

    @Test
    public void upsertAndRead() throws InterruptedException {
        _writer.startup();
        _reader.startup(_clientOptions);

        IKVDocument document = new IKVDocument.Builder()
                .putStringField("userid", "firstuserid")
                .build();
        _writer.upsertFieldValues(document);  // TODO: this blocks even after server returns!

        Thread.sleep(1000 * 5);  // 5 sec sleep

        // Read

        String value = _reader.getStringValue(PrimaryKey.from("firstuserid"), USERID_ACCESSOR);
        Assertions.assertEquals(value, "firstuserid");
    }
}
