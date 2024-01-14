package io.inline.gateway.streaming;

import com.inlineio.schemas.Common;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

// TODO: pending code review.
public class KafkaProducerFactory {
  // TODO: consider storing this per store in dynamodb
  public static final String KAFKA_BOOTSTRAP_SERVER =
      "b-2.mskcluster1.yz62h3.c5.kafka.us-west-2.amazonaws.com:9098";

  public static <R> Producer<Common.FieldValue, R> createInstance() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVER);
    props.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "io.inline.gateway.streaming.SimpleProtoSerializer");
    props.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "io.inline.gateway.streaming.SimpleProtoSerializer");
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 0); // TODO - remove

    // iam authentication
    // ref: https://github.com/aws/aws-msk-iam-auth
    // ref: https://docs.aws.amazon.com/msk/latest/developerguide/iam-access-control.html
    props.put("security.protocol", "SASL_SSL");
    props.put("sasl.mechanism", "AWS_MSK_IAM");
    props.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
    props.put(
        "sasl.client.callback.handler.class",
        "software.amazon.msk.auth.iam.IAMClientCallbackHandler");

    // see props for option to add custom partitioner
    // props.put("schema.registry.url", "http://127.0.0.1:8081");

    // topic names are not assigned to a producer

    return new KafkaProducer<>(props);
  }
}
