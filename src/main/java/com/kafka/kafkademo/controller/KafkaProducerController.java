package com.kafka.kafkademo.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.kafkademo.model.TopicMessageA;
import com.kafka.kafkademo.model.TopicMessageB;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/produce")
public class KafkaProducerController {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${kafka.topics.inputA:TOPIC_A}")
    private String topicA;

    @Value("${kafka.topics.inputB:TOPIC_B}")
    private String topicB;

    public KafkaProducerController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/topicA")
    public String produceToTopicA(@RequestBody TopicMessageA message) throws Exception {
        String key = message.key.catalog_number + "-" + message.key.country;
        kafkaTemplate.send(topicA, key, objectMapper.writeValueAsString(message));
        return "Message sent to TOPIC_A";
    }

    @PostMapping("/topicB")
    public String produceToTopicB(@RequestBody TopicMessageB message) throws Exception {
        String key = message.key.catalog_number + "-" + message.key.country;
        kafkaTemplate.send(topicB, key, objectMapper.writeValueAsString(message));
        return "Message sent to TOPIC_B";
    }
}
