package io.inline.gateway.streaming;

import com.google.protobuf.Message;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

/** Custom */
public class SimpleProtoSerializer<T extends Message> implements Serializer<T> {
    @Override
    public byte[] serialize(String topic, T message) {
        // proto buf schemas don't need to be managed in a registry
        return message.toByteArray();
    }

    @Override
    public byte[] serialize(String topic, Headers headers, T data) {
        return Serializer.super.serialize(topic, headers, data);
    }
}
