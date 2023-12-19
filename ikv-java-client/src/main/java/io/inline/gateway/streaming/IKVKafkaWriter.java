package io.inline.gateway.streaming;

import com.google.common.base.Preconditions;
import com.inlineio.schemas.Common.*;
import com.inlineio.schemas.Streaming.*;
import io.inline.gateway.UserStoreContext;
import java.util.*;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class IKVKafkaWriter {
  private static final Logger LOGGER = LogManager.getLogger(IKVKafkaWriter.class);
  private final Producer<FieldValue, IKVDataEvent> _kafkaProducer;

  public IKVKafkaWriter(Producer<FieldValue, IKVDataEvent> kafkaProducer) {
    _kafkaProducer = Objects.requireNonNull(kafkaProducer);
  }

  /**
   * Partition and publish collection of document updates to downstream kafka.
   *
   * @param context details of user InlineKV store
   * @param documents field names vs field values - for upserts
   * @throws NullPointerException missing/null required request parameters
   * @throws IllegalArgumentException primary or partitioning keys unavailable
   * @throws RuntimeException kafka write errors
   */
  public void publishDocumentUpserts(
      UserStoreContext context, Collection<Map<String, FieldValue>> documents) {
    Objects.requireNonNull(context);
    Objects.requireNonNull(documents);
    if (documents.isEmpty()) {
      return;
    }

    for (Map<String, FieldValue> document : documents) {

      // extract primary key value for validation
      Objects.requireNonNull(extractPrimaryKeyValue(context, document));

      // extract partitioning key for kafka
      FieldValue kafkaPartitioningKey =
          Objects.requireNonNull(extractPartitioningKeyValue(context, document));

      // TODO: IMPORTANT (refactor as per schema-less design) !! filter out unknown fields by
      // fetching schema
      IKVDocumentOnWire ikvDocumentOnWire =
          IKVDocumentOnWire.newBuilder().putAllDocument(document).build();

      IKVDataEvent event =
          IKVDataEvent.newBuilder()
              .setEventHeader(EventHeader.newBuilder().build())
              .setUpsertDocumentFieldsEvent(
                  UpsertDocumentFieldsEvent.newBuilder().setDocument(ikvDocumentOnWire).build())
              .build();

      // ProducerRecord(String topic, K key, V value)
      ProducerRecord<FieldValue, IKVDataEvent> producerRecord =
          new ProducerRecord<>(context.kafkaTopic(), kafkaPartitioningKey, event);

      publishToKafkaWithRetries(producerRecord, 3);
    }
  }

  /**
   * Partition and publish collection of document updates to downstream kafka.
   *
   * @param context details of user InlineKV store
   * @param documentIds maps containing primary & partitioning key values
   * @throws NullPointerException missing/null required request parameters
   * @throws IllegalArgumentException primary or partitioning keys unavailable
   * @throws RuntimeException kafka write errors
   */
  public void publishDocumentDeletes(
      UserStoreContext context, Collection<Map<String, FieldValue>> documentIds) {
    Objects.requireNonNull(context);
    Objects.requireNonNull(documentIds);
    if (documentIds.isEmpty()) {
      return;
    }

    for (Map<String, FieldValue> documentId : documentIds) {
      // project document identifiers
      String primaryKeyFieldName = context.primaryKeyFieldName();
      String partitioningKeyFieldName = context.partitioningKeyFieldName();

      FieldValue primaryKey = documentId.get(primaryKeyFieldName);
      Preconditions.checkArgument(primaryKey != null, "Cannot delete without primary-key");

      FieldValue partitioningKey = documentId.get(partitioningKeyFieldName);
      Preconditions.checkArgument(
          partitioningKey != null, "Cannot delete without partitioning-key");

      IKVDocumentOnWire documentIdOnWire =
          IKVDocumentOnWire.newBuilder()
              .putDocument(primaryKeyFieldName, primaryKey)
              .putDocument(partitioningKeyFieldName, partitioningKey)
              .build();

      IKVDataEvent event =
          IKVDataEvent.newBuilder()
              .setEventHeader(EventHeader.newBuilder().build())
              .setDeleteDocumentEvent(
                  DeleteDocumentEvent.newBuilder().setDocumentId(documentIdOnWire).build())
              .build();

      // ProducerRecord(String topic, K key, V value)
      ProducerRecord<FieldValue, IKVDataEvent> producerRecord =
          new ProducerRecord<>(context.kafkaTopic(), partitioningKey, event);

      publishToKafkaWithRetries(producerRecord, 3);
    }
  }

  // Broadcast schema updates to all partitions for reader clients.
  // Ok to fail partially and propagate error.
  @Deprecated
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
      publishToKafkaWithRetries(producerRecord, 3); // can throw
    }
  }

  /**
   * Write to kafka with retires.
   *
   * @param record to publish
   * @param numRetries for error handling
   * @throws RuntimeException for kafka write errors (after all retries are exhausted)
   */
  private void publishToKafkaWithRetries(
      ProducerRecord<FieldValue, IKVDataEvent> record, int numRetries) {
    Objects.requireNonNull(record);
    Preconditions.checkArgument(numRetries > 0);

    RuntimeException error = null;

    for (int i = 0; i < numRetries; i++) {
      try {
        // Blocking send() - to provide some back pressure when there
        // is a spike in write traffic.
        // TODO: inspect kafka batch size configs for the producer
        _kafkaProducer.send(record).get();
        return;
      } catch (InterruptedException | ExecutionException e) {
        error = new RuntimeException(e);
      } catch (RuntimeException e) {
        error = e;
      }

      // TODO: consider using waited retires (github Retryer?)
    }

    // All writes attempts failed.
    LOGGER.error("All kafka write attempts failed, error: ", error);
    throw error;
  }

  private static FieldValue extractPrimaryKeyValue(
      UserStoreContext context, Map<String, FieldValue> document) throws IllegalArgumentException {
    FieldValue value = document.get(context.primaryKeyFieldName());
    Preconditions.checkArgument(value != null, "primaryKey missing");
    return value;
  }

  private static FieldValue extractPartitioningKeyValue(
      UserStoreContext context, Map<String, FieldValue> document) throws IllegalArgumentException {
    FieldValue value = document.get(context.partitioningKeyFieldName());
    Preconditions.checkArgument(value != null, "partitioningKey missing");
    return value;
  }
}