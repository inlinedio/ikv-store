package io.inline.gateway.streaming;

import com.inlineio.schemas.Common;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

// TODO: pending code review.
public class KafkaProducerFactory {
  public static final String KAFKA_BOOTSTRAP_SERVER = "127.0.0.1:9092";

  public static <R> Producer<Common.FieldValue, R> createInstance() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVER);
    props.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    props.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "io.inline.gateway.streaming.SimpleProtoSerializer");
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 0); // TODO - remove

    // see props for option to add custom partitioner
    // props.put("schema.registry.url", "http://127.0.0.1:8081");

    // topic names are not assigned to a producer

    return new KafkaProducer<>(props);
  }
}
