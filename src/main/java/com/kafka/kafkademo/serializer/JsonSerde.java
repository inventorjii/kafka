package com.kafka.kafkademo.serializer;

import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class JsonSerde<T> extends Serdes.WrapperSerde<T> {
    public JsonSerde(Class<T> type) {
        super(new JsonSerializer<>(), new JsonDeserializer<>(type));
    }
}
