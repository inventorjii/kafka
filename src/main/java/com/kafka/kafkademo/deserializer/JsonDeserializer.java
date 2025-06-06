package com.kafka.kafkademo.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

public class JsonDeserializer<T> implements Deserializer<T> {
    private final ObjectMapper mapper = new ObjectMapper();
    private final Class<T> targetType;

    public JsonDeserializer(Class<T> targetType) {
        this.targetType = targetType;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            return mapper.readValue(data, targetType);
        } catch (Exception e) {
            throw new RuntimeException("JSON deserialization error", e);
        }
    }
}