package io.inline;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class IntegrationTests {
    @Test
    public void openAndClose() {
        IKVClient ikvClient =
                IKVClient.create_new("/tmp/openAndClose", "/Users/pushkar/projects/inlineio/ikv/src/schema/sample.yaml");
        ikvClient.close();
    }

    @Test
    public void basic() {
        IKVClient ikvClient =
                IKVClient.create_new("/tmp/basic", "/Users/pushkar/projects/inlineio/ikv/src/schema/sample.yaml");

        byte[] docId1 = "document1".getBytes(StandardCharsets.UTF_8);
        byte[] firstname1 = "alice".getBytes(StandardCharsets.UTF_8);

        byte[] docId2 = "document2".getBytes(StandardCharsets.UTF_8);
        byte[] age2 = ByteBuffer.allocate(4).putInt(25).array();

        byte[] docId3 = "document3".getBytes(StandardCharsets.UTF_8);
        byte[] profile3 = "profileBytes".getBytes(StandardCharsets.UTF_8);

        // not inserted
        byte[] docId4 = "document4".getBytes(StandardCharsets.UTF_8);

        // WRITES
        ikvClient.upsertFieldValue(docId1, firstname1, "firstname");

        byte[] val = ikvClient.getBytesFieldValue(docId1, "firstname");
        Assertions.assertArrayEquals(val, firstname1);

        ikvClient.upsertFieldValue(docId2, age2, "age");
        ikvClient.upsertFieldValue(docId3, profile3, "profile");

        // READS
        val = ikvClient.getBytesFieldValue(docId1, "firstname");
        Assertions.assertArrayEquals(val, firstname1);

        val = ikvClient.getBytesFieldValue(docId2, "age");
        Assertions.assertArrayEquals(val, age2);

        val = ikvClient.getBytesFieldValue(docId3, "profile");
        Assertions.assertArrayEquals(val, profile3);

        Assertions.assertNull(ikvClient.getBytesFieldValue(docId4, "firstname"));

        ikvClient.close();
    }
}
