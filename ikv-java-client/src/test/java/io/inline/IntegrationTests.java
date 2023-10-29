package io.inline;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class IntegrationTests {
    private static final String INDEX_SCHEMA = "document:\n" +
            "  - name: firstname\n" +
            "    id: 2\n" +
            "    type: string\n" +
            "  - name: age\n" +
            "    id: 0\n" +
            "    type: i32\n" +
            "  - name: profile\n" +
            "    id: 1\n" +
            "    type: bytes\n" +
            "  - name: zip\n" +
            "    id: 3\n" +
            "    type: i32";

    @Test
    public void openAndClose() {
        IKVClient ikvClient =
                IKVClient.create_new("/tmp/openAndClose", INDEX_SCHEMA);
        ikvClient.close();
    }

    @Test
    public void basic() {
        IKVClient ikvClient =
                IKVClient.create_new("/tmp/basic", INDEX_SCHEMA);

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

        ByteBuffer val = ikvClient.getFieldValue(docId1, "firstname");
        Assertions.assertArrayEquals(val.array(), firstname1);

        ikvClient.upsertFieldValue(docId2, age2, "age");
        ikvClient.upsertFieldValue(docId3, profile3, "profile");

        // READS
        val = ikvClient.getFieldValue(docId1, "firstname");
        Assertions.assertArrayEquals(val.array(), firstname1);

        val = ikvClient.getFieldValue(docId2, "age");
        Assertions.assertArrayEquals(val.array(), age2);

        val = ikvClient.getFieldValue(docId3, "profile");
        Assertions.assertArrayEquals(val.array(), profile3);

        Assertions.assertNull(ikvClient.getFieldValue(docId4, "firstname"));

        ikvClient.close();
    }
}
