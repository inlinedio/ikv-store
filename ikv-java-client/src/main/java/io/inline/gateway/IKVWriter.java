package io.inline.gateway;

import com.google.common.base.Preconditions;
import com.inlineio.schemas.Common.*;
import com.inlineio.schemas.Streaming.*;
import io.inline.gateway.streaming.KafkaProducerFactory;
import java.util.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class IKVWriter {
  private static final FieldValue BROADCAST_KEY =
      FieldValue.newBuilder().build(); // empty no op key

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
  public void publishFieldUpserts(
      UserStoreContext context, Collection<Map<String, FieldValue>> fieldMaps)
      throws IllegalArgumentException, InterruptedException {
    Preconditions.checkNotNull(context);
    Preconditions.checkNotNull(fieldMaps);
    if (fieldMaps.isEmpty()) {
      return;
    }

    for (Map<String, FieldValue> fieldMap : fieldMaps) {

      // extract primary key value for validation
      Preconditions.checkNotNull(ExtractorUtils.extractPrimaryKeyAsString(context, fieldMap));
      FieldValue kafkaPartitioningKey =
          ExtractorUtils.extractPartitioningKeyValue(context, fieldMap);

      // Very important! We need to remove unknown fields to
      // avoid readers from halting.
      // TODO: filter out unknown fields by fetching schema
      IKVDocumentOnWire document = IKVDocumentOnWire.newBuilder().putAllDocument(fieldMap).build();

      IKVDataEvent event =
          IKVDataEvent.newBuilder()
              .setEventHeader(EventHeader.newBuilder().build())
              .setUpsertDocumentFieldsEvent(
                  UpsertDocumentFieldsEvent.newBuilder().setDocument(document).build())
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
  public void publishDocumentDeletes(
      UserStoreContext context, Collection<Map<String, FieldValue>> documentIds)
      throws IllegalArgumentException, InterruptedException {
    Preconditions.checkNotNull(context);
    if (documentIds == null || documentIds.isEmpty()) {
      return;
    }

    for (Map<String, FieldValue> documentId : documentIds) {
      // extract primary key value for validation
      Preconditions.checkNotNull(ExtractorUtils.extractPrimaryKeyAsString(context, documentId));

      FieldValue kafkaPartitioningKey =
          ExtractorUtils.extractPartitioningKeyValue(context, documentId);

      // TODO: filter out unknown fields by fetching schema
      IKVDocumentOnWire documentIdOnWire =
          IKVDocumentOnWire.newBuilder().putAllDocument(documentId).build();

      IKVDataEvent event =
          IKVDataEvent.newBuilder()
              .setEventHeader(EventHeader.newBuilder().build())
              .setDeleteDocumentEvent(
                  DeleteDocumentEvent.newBuilder().setDocumentId(documentIdOnWire).build())
              .build();

      // ProducerRecord(String topic, K key, V value)
      ProducerRecord<FieldValue, IKVDataEvent> producerRecord =
          new ProducerRecord<>(context.kafkaTopic(), kafkaPartitioningKey, event);

      blockingPublishWithRetries(producerRecord, 3);
    }
  }

  // Broadcast schema updates to all partitions for reader clients.
  // Ok to fail partially and propagate error.
  public void publishFieldSchemaUpdates(
      UserStoreContext context, Collection<FieldSchema> newFieldsToAdd)
      throws InterruptedException {
    Preconditions.checkNotNull(context);
    if (newFieldsToAdd == null || newFieldsToAdd.isEmpty()) {
      return;
    }

    IKVDataEvent event =
        IKVDataEvent.newBuilder()
            .setEventHeader(EventHeader.newBuilder().build())
            .setUpdateFieldSchemaEvent(
                UpdateFieldSchemaEvent.newBuilder().addAllNewFieldsToAdd(newFieldsToAdd).build())
            .build();

    // Broadcast!
    int numPartitions = context.numKafkaPartitions();
    for (int i = 0; i < numPartitions; i++) {
      ProducerRecord<FieldValue, IKVDataEvent> producerRecord =
          new ProducerRecord<>(context.kafkaTopic(), 0, null, event);
      blockingPublishWithRetries(producerRecord, 3); // can throw
    }
  }

  private void blockingPublishWithRetries(
      ProducerRecord<FieldValue, IKVDataEvent> record, int numRetries) throws InterruptedException {
    for (int i = 0; i < numRetries; i++) {
      try {
        _kafkaProducer.send(record).get();
        return;
      } catch (Exception e) {
        // TODO: add logging
        System.out.println("Write to kafka error: " + e);
      }
    }
  }
}
