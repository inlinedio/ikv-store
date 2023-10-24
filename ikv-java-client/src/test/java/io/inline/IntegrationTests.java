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
        byte[] profile3 = "profileBytes".getBytes(StandardCharsets.UTF_8);

        // WRITES
        ikvClient.upsertFieldValue("document1", "alice".getBytes(StandardCharsets.UTF_8), "firstname");

        // get for document1
        Assertions.assertEquals( "alice", ikvClient.getStringFieldValue("document1", "firstname"));

        ikvClient.upsertFieldValue("document3", profile3, "profile");

        // get for document1
        Assertions.assertEquals("alice", ikvClient.getStringFieldValue("document1", "firstname"));

        // get for document3
        Assertions.assertArrayEquals(profile3, ikvClient.getBytesFieldValue("document3", "profile"));

        // get for document2
        Assertions.assertNull(ikvClient.getBytesFieldValue("document2", "firstname"));

        ikvClient.close();
    }
}
