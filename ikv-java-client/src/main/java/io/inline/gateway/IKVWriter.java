package io.inline.gateway;

import com.google.common.base.Preconditions;
import com.inlineio.schemas.Common.*;
import com.inlineio.schemas.Streaming.*;
import io.inline.gateway.streaming.KafkaProducerFactory;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.util.*;

public class IKVWriter {
    private final Producer<FieldValue, IKVDataEvent> _kafkaProducer;

    public IKVWriter() {
        _kafkaProducer = KafkaProducerFactory.createInstance();
    }

    /**
     * Partition and publish collection of document updates to downstream kafka.
     *
     * @param context details of user InlineKV store
     * @param fieldMaps field names vs field values - for upserts
     * @throws IllegalArgumentException primary or partitioning keys unavailable
     * @throws InterruptedException kafka publisher is interrupted
     * @throws NullPointerException null arguments
     */
    public void publishFieldUpserts(UserStoreContext context, Collection<Map<String, FieldValue>> fieldMaps) throws IllegalArgumentException, InterruptedException {
        Preconditions.checkNotNull(context);
        Preconditions.checkNotNull(fieldMaps);
        if (fieldMaps.isEmpty()) {
            return;
        }

        for (Map<String, FieldValue> fieldMap : fieldMaps) {

            // extract primary key value for validation
            Preconditions.checkNotNull(ExtractorUtils.extractPrimaryKeyAsString(context, fieldMap));
            FieldValue kafkaPartitioningKey = ExtractorUtils.extractPartitioningKeyValue(context, fieldMap);

            // TODO: filter out unknown fields by fetching schema
            IKVDocumentOnWire document = IKVDocumentOnWire.newBuilder().putAllDocument(fieldMap).build();

            IKVDataEvent event = IKVDataEvent.newBuilder()
                    .setEventHeader(EventHeader
                            .newBuilder()
                            .build())
                    .addAllFieldSchema(createFieldSchemaList(context, document))
                    .setUpsertDocumentFieldsEvent(
                            UpsertDocumentFieldsEvent
                                    .newBuilder()
                                    .setDocument(document)
                                    .build())
                    .build();

            // ProducerRecord(String topic, K key, V value)
            ProducerRecord<FieldValue, IKVDataEvent> producerRecord =
                    new ProducerRecord<>(context.kafkaTopic(), kafkaPartitioningKey, event);

            blockingPublishWithRetries(producerRecord, 3);

            System.out.println("Published to kafka: " + event.toString());
        }
    }

    /**
     * Partition and publish collection of document updates to downstream kafka.
     *
     * @param context details of user InlineKV store
     * @param documentIds maps containing primary & partitioning key values
     * @throws IllegalArgumentException primary or partitioning keys unavailable
     * @throws InterruptedException kafka publisher is interrupted
     * @throws NullPointerException null arguments
     */
    public void publishDocumentDeletes(UserStoreContext context, Collection<Map<String, FieldValue>> documentIds) throws IllegalArgumentException, InterruptedException {
        Preconditions.checkNotNull(context);
        Preconditions.checkNotNull(documentIds);
        if (documentIds.isEmpty()) {
            return;
        }

        for (Map<String, FieldValue> documentId : documentIds) {
            // extract primary key value for validation
            Preconditions.checkNotNull(ExtractorUtils.extractPrimaryKeyAsString(context, documentId));

            FieldValue kafkaPartitioningKey = ExtractorUtils.extractPartitioningKeyValue(context, documentId);

            // TODO: filter out unknown fields by fetching schema
            IKVDocumentOnWire documentIdOnWire = IKVDocumentOnWire.newBuilder().putAllDocument(documentId).build();

            IKVDataEvent event = IKVDataEvent.newBuilder()
                    .setEventHeader(EventHeader
                            .newBuilder()
                            .build())
                    .addAllFieldSchema(createFieldSchemaList(context, documentIdOnWire))
                    .setDeleteDocumentEvent(
                            DeleteDocumentEvent
                                    .newBuilder()
                                    .setDocumentId(documentIdOnWire)
                                    .build())
                    .build();

            // ProducerRecord(String topic, K key, V value)
            ProducerRecord<FieldValue, IKVDataEvent> producerRecord =
                    new ProducerRecord<>(context.kafkaTopic(), kafkaPartitioningKey, event);

            blockingPublishWithRetries(producerRecord, 3);
        }
    }

    /**
     * Construct field schema object based on the downstream event.
     */
    private static List<FieldSchema> createFieldSchemaList(UserStoreContext context, IKVDocumentOnWire document) {
        Map<String, ?> documentMap = document.getDocumentMap();
        List<FieldSchema> schema = new ArrayList<>(documentMap.size());

        for (String name : documentMap.keySet()) {
            Optional<FieldSchema> maybeSchema = context.fieldSchema(name);
            maybeSchema.ifPresent(schema::add);
        }

        return schema;
    }


    private void blockingPublishWithRetries(ProducerRecord<FieldValue, IKVDataEvent> record, int numRetries) throws InterruptedException {
        for (int i = 0; i < numRetries; i++) {
            try {
                // TODO block on get() - can be problematic based on batching
                _kafkaProducer.send(record).get();

                return;
            } catch (Exception e) {
                // TODO: add logging
                System.out.println("Write to kafka error: " + e);
            }
        }
    }


}
