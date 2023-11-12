package io.inline.gateway.streaming;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class KafkaProducerFactory {

    public static <R> Producer<String, R> createInstance() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "io.inline.gateway.streaming.SimpleProtoSerializer");

        // see props for option to add custom partitioner
        // props.put("schema.registry.url", "http://127.0.0.1:8081");

        // topic names are not assigned to a producer

        return new KafkaProducer<>(props);
    }
}
