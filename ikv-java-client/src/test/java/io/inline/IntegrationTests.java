package io.inline;

import com.google.common.collect.ImmutableList;
import com.inlineio.schemas.Common.IKVStoreConfig;
import io.inline.clients.LegacyIKVClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static io.inline.clients.LegacyIKVClient.*;

public class IntegrationTests {
    private IKVStoreConfig _config;

    @BeforeAll
    public void setup() {
        _config = IKVStoreConfig.newBuilder()
                .putStringConfigs(CFG_MOUNT_DIRECTORY, "/tmp/JavaIntegrationTests")
                .putStringConfigs(CFG_PRIMARY_KEY, "documentId")
                .putStringConfigs(CFG_KAFKA_BOOTSTRAP_SERVER, "localhost")
                .putStringConfigs(CFG_KAFKA_TOPIC, "topic")
                .putNumericConfigs(CFG_KAFKA_PARTITION, 0L)
                .build();
    }

    //@Test
    public void openAndClose() {
        LegacyIKVClient legacyIkvClient = LegacyIKVClient.open(_config);
        legacyIkvClient.close();
    }

    //@Test
    public void basic() {
        LegacyIKVClient legacyIkvClient = LegacyIKVClient.open(_config);

        byte[] docId1 = "document1".getBytes(StandardCharsets.UTF_8);
        byte[] firstname1 = "alice".getBytes(StandardCharsets.UTF_8);

        byte[] docId2 = "document2".getBytes(StandardCharsets.UTF_8);
        byte[] age2 = ByteBuffer.allocate(4).putInt(25).array();

        byte[] docId3 = "document3".getBytes(StandardCharsets.UTF_8);
        byte[] profile3 = "profileBytes".getBytes(StandardCharsets.UTF_8);

        // not inserted
        byte[] docId4 = "document4".getBytes(StandardCharsets.UTF_8);

        // WRITES
        // FAILING here!!!
        legacyIkvClient.upsertFieldValue(docId1, firstname1, "firstname");

        byte[] val = legacyIkvClient.readBytesField(docId1, "firstname");
        Assertions.assertArrayEquals(val, firstname1);

        legacyIkvClient.upsertFieldValue(docId2, age2, "age");
        legacyIkvClient.upsertFieldValue(docId3, profile3, "profile");

        // READS
        val = legacyIkvClient.readBytesField(docId1, "firstname");
        Assertions.assertArrayEquals(val, firstname1);

        val = legacyIkvClient.readBytesField(docId2, "age");
        Assertions.assertArrayEquals(val, age2);

        val = legacyIkvClient.readBytesField(docId3, "profile");
        Assertions.assertArrayEquals(val, profile3);

        Assertions.assertNull(legacyIkvClient.readBytesField(docId4, "firstname"));


        Assertions.assertNotNull(
                legacyIkvClient.batchReadBytesField(
                        ImmutableList.of(docId1, docId2, docId3), "firstname"));

        legacyIkvClient.close();
    }
}
