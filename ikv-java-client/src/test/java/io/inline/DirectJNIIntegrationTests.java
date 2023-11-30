package io.inline;

import com.google.common.collect.ImmutableList;
import io.inline.clients.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

public class DirectJNIIntegrationTests {
    private static final FieldAccessor NAME_FIELD_ACCESSOR = FieldAccessor.stringFieldAccessor("name");
    private static final FieldAccessor PROFILE_FIELD_ACCESSOR = FieldAccessor.bytesFieldAccessor("profile");


    private final ClientOptions _clientOptions = new ClientOptions.Builder()
            .withMountDirectory("/tmp/JavaIntegrationTests")
            .withPrimaryKeyFieldName("key")
            .withStoreName("JavaIntegrationTests")
            .withKafkaConsumerBootstrapServer("localhost")
            .withKafkaConsumerTopic("JavaIntegrationTests")
            .withKafkaConsumerPartition(0)
            .build();

    //@Test
    public void openAndClose() {
        InlineKVReader client = new TestingInlineKVReader(_clientOptions);
        client.startup(_clientOptions);
        client.shutdown();
    }

    //@Test
    public void singleAndBatchReads() {
        TestingInlineKVReader client = new TestingInlineKVReader(_clientOptions);
        client.startup(_clientOptions);

        // document1
        byte[] key1 = "key1".getBytes(StandardCharsets.UTF_8);
        String name1 = "alice";
        byte[] profile1 = "profile1".getBytes(StandardCharsets.UTF_8);

        // document2
        byte[] key2 = "key2".getBytes(StandardCharsets.UTF_8);
        String name2 = "bob";
        byte[] profile2 = "profile2".getBytes(StandardCharsets.UTF_8);

        // document3
        byte[] key3 = "key3".getBytes(StandardCharsets.UTF_8);
        String name3 = "sam";
        // no profile field

        // not inserted
        byte[] key4 = "key4".getBytes(StandardCharsets.UTF_8);

        // WRITE doc1
        IKVDocument document = new IKVDocument.Builder()
                .putBytesField("key", key1)
                        .putStringField("name", name1)
                                .putBytesField("profile", profile1)
                                        .build();
        client.upsertFieldValues(document);

        // READS on doc1
        Assertions.assertEquals(name1,
                client.getStringValue(PrimaryKey.from(key1), NAME_FIELD_ACCESSOR));
        Assertions.assertArrayEquals(profile1,
                client.getBytesValue(PrimaryKey.from(key1), PROFILE_FIELD_ACCESSOR));

        // WRITE doc2 and doc3
        document = new IKVDocument.Builder()
                .putBytesField("key", key2)
                .putStringField("name", name2)
                .putBytesField("profile", profile2)
                .build();
        client.upsertFieldValues(document);

        document = new IKVDocument.Builder()
                .putBytesField("key", key3)
                .putStringField("name", name3)
                .build();
        client.upsertFieldValues(document);

        // READS on doc2
        Assertions.assertEquals(name2,
                client.getStringValue(PrimaryKey.from(key2), NAME_FIELD_ACCESSOR));
        Assertions.assertArrayEquals(profile2,
                client.getBytesValue(PrimaryKey.from(key2), PROFILE_FIELD_ACCESSOR));

        // READS on doc3
        Assertions.assertEquals(name3,
                client.getStringValue(PrimaryKey.from(key3), NAME_FIELD_ACCESSOR));
        Assertions.assertNull(client.getBytesValue(PrimaryKey.from(key3), PROFILE_FIELD_ACCESSOR));


        // BATCH READ
        List<PrimaryKey> keys = ImmutableList.of(PrimaryKey.from(key1), PrimaryKey.from(key2), PrimaryKey.from(key3));

        List<String> names = client.multiGetStringValue(keys, NAME_FIELD_ACCESSOR);
        Assertions.assertArrayEquals(names.toArray(new String[0]), new String[]{name1, name2, name3});

        List<byte[]> profiles = client.multiGetBytesValue(keys, PROFILE_FIELD_ACCESSOR);
        Assertions.assertArrayEquals(profiles.toArray(new byte[0][]), new byte[][]{profile1, profile2, null});

        client.shutdown();
    }

    //@Test
    public void deletes() {
        TestingInlineKVReader client = new TestingInlineKVReader(_clientOptions);
        client.startup(_clientOptions);

        // document1
        byte[] key1 = "key1".getBytes(StandardCharsets.UTF_8);
        String name1 = "alice";
        byte[] profile1 = "profile1".getBytes(StandardCharsets.UTF_8);

        // document2
        byte[] key2 = "key2".getBytes(StandardCharsets.UTF_8);
        String name2 = "bob";
        byte[] profile2 = "profile2".getBytes(StandardCharsets.UTF_8);

        // WRITE doc2 and doc3
        IKVDocument document1 = new IKVDocument.Builder()
                .putBytesField("key", key1)
                .putStringField("name", name1)
                .putBytesField("profile", profile1)
                .build();
        client.upsertFieldValues(document1);

        IKVDocument document2 = new IKVDocument.Builder()
                .putBytesField("key", key2)
                .putStringField("name", name2)
                .putBytesField("profile", profile2)
                .build();
        client.upsertFieldValues(document2);

        // READS on doc1 and doc2
        Assertions.assertEquals(name1,
                client.getStringValue(PrimaryKey.from(key1), NAME_FIELD_ACCESSOR));
        Assertions.assertEquals(name2,
                client.getStringValue(PrimaryKey.from(key2), NAME_FIELD_ACCESSOR));

        // DELETE all doc1, name for doc2
        client.deleteDocument(document1);
        client.deleteFieldValues(document2, Collections.singleton("name"));

        // all null for doc1
        Assertions.assertNull(client.getStringValue(PrimaryKey.from(key1), NAME_FIELD_ACCESSOR));
        Assertions.assertNull(client.getBytesValue(PrimaryKey.from(key1), PROFILE_FIELD_ACCESSOR));

        // name null, profile not-null for doc2
        Assertions.assertNull(client.getStringValue(PrimaryKey.from(key2), NAME_FIELD_ACCESSOR));
        Assertions.assertArrayEquals(profile2,
                client.getBytesValue(PrimaryKey.from(key2), PROFILE_FIELD_ACCESSOR));
    }
}
