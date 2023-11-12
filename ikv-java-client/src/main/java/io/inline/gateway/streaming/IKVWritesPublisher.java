package io.inline.gateway.streaming;

import com.google.common.base.Preconditions;
import com.inlineio.schemas.Services.*;
import com.inlineio.schemas.Streaming.*;
import io.inline.gateway.ExtractorUtils;
import io.inline.gateway.UserStoreContext;
import io.inline.gateway.streaming.KafkaProducerFactory;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class IKVWritesPublisher {
    private final Producer<String, IKVDataEvent> _kafkaProducer;

    public IKVWritesPublisher() {
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

            String kafkaPartitioningKey = ExtractorUtils.extractPartitioningKeyAsString(context, fieldMap);
            MultiFieldDocument multiFieldDocument = MultiFieldDocument.newBuilder().putAllDocument(fieldMap).build();
            IKVDataEvent event = IKVDataEvent.newBuilder()
                    .setEventHeader(EventHeader
                            .newBuilder()
                            .build())
                    .setUpsertDocumentFieldsEvent(
                            UpsertDocumentFieldsEvent
                                    .newBuilder()
                                    .setMultiFieldDocument(multiFieldDocument)
                                    .build())
                    .build();

            // ProducerRecord(String topic, K key, V value)
            ProducerRecord<String, IKVDataEvent> producerRecord =
                    new ProducerRecord<>(context.kafkaTopicName(), kafkaPartitioningKey, event);

            blockingPublishWithRetries(producerRecord, 3);
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

            String kafkaPartitioningKey = ExtractorUtils.extractPartitioningKeyAsString(context, documentId);
            MultiFieldDocument multiFieldDocument = MultiFieldDocument.newBuilder().putAllDocument(documentId).build();

            IKVDataEvent event = IKVDataEvent.newBuilder()
                    .setEventHeader(EventHeader
                            .newBuilder()
                            .build())
                    .setDeleteDocumentEvent(
                            DeleteDocumentEvent
                                    .newBuilder()
                                    .setDocumentId(multiFieldDocument)
                                    .build())
                    .build();

            // ProducerRecord(String topic, K key, V value)
            ProducerRecord<String, IKVDataEvent> producerRecord =
                    new ProducerRecord<>(context.kafkaTopicName(), kafkaPartitioningKey, event);

            blockingPublishWithRetries(producerRecord, 3);
        }
    }




    private void blockingPublishWithRetries(ProducerRecord<String, IKVDataEvent> record, int numRetries) throws InterruptedException {
        for (int i = 0; i < numRetries; i++) {
            try {
                _kafkaProducer.send(record).get();
                return;
            } catch (ExecutionException ignored) {
                // TODO: add logging
            }
        }
    }


}
