package io.inline;

import com.google.common.collect.ImmutableList;
import io.inline.clients.LegacyIKVClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class IntegrationTests {
    //@Test
    public void openAndClose() {
        LegacyIKVClient legacyIkvClient =
                LegacyIKVClient.createNew("/tmp/openAndClose", "/Users/pushkar/projects/inlineio/ikv/src/schema/sample.yaml");
        legacyIkvClient.close();
    }

    //@Test
    public void basic() {
        LegacyIKVClient legacyIkvClient =
                LegacyIKVClient.createNew("/tmp/basic", "/Users/pushkar/projects/inlineio/ikv/src/schema/sample.yaml");

        byte[] docId1 = "document1".getBytes(StandardCharsets.UTF_8);
        byte[] firstname1 = "alice".getBytes(StandardCharsets.UTF_8);

        byte[] docId2 = "document2".getBytes(StandardCharsets.UTF_8);
        byte[] age2 = ByteBuffer.allocate(4).putInt(25).array();

        byte[] docId3 = "document3".getBytes(StandardCharsets.UTF_8);
        byte[] profile3 = "profileBytes".getBytes(StandardCharsets.UTF_8);

        // not inserted
        byte[] docId4 = "document4".getBytes(StandardCharsets.UTF_8);

        // WRITES
        legacyIkvClient.upsertFieldValue(docId1, firstname1, "firstname");

        byte[] val = legacyIkvClient.getBytesFieldValue(docId1, "firstname");
        Assertions.assertArrayEquals(val, firstname1);

        legacyIkvClient.upsertFieldValue(docId2, age2, "age");
        legacyIkvClient.upsertFieldValue(docId3, profile3, "profile");

        // READS
        val = legacyIkvClient.getBytesFieldValue(docId1, "firstname");
        Assertions.assertArrayEquals(val, firstname1);

        val = legacyIkvClient.getBytesFieldValue(docId2, "age");
        Assertions.assertArrayEquals(val, age2);

        val = legacyIkvClient.getBytesFieldValue(docId3, "profile");
        Assertions.assertArrayEquals(val, profile3);

        Assertions.assertNotNull(
                legacyIkvClient.getBatchBytesFieldValue(
                        ImmutableList.of(docId1, docId2, docId3), "firstname"));

        Assertions.assertNull(legacyIkvClient.getBytesFieldValue(docId4, "firstname"));

        legacyIkvClient.close();
    }
}
