package com.kafka.kafkademo.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.kafkademo.model.*;
import com.kafka.kafkademo.serializer.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;


@Configuration
public class JoinStreamResults {

    private static final Logger logger = LoggerFactory.getLogger(JoinStreamResults.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${kafka.topics.inputA:TOPIC_A}")
    private String topicA;

    @Value("${kafka.topics.inputB:TOPIC_B}")
    private String topicB;

    @Value("${kafka.topics.output:TOPIC_C}")
    private String topicC;

    @Value("${kafka.topics.dlq:TOPIC_DLQ}")
    private String dlqTopic;

    @Bean
    public KStream<String, ?> kStream(StreamsBuilder builder) {
        JsonSerde<TopicMessageA> wrapperASerde = new JsonSerde<>(TopicMessageA.class);
        JsonSerde<TopicMessageB> wrapperBSerde = new JsonSerde<>(TopicMessageB.class);
        JsonSerde<TopicValueA> valueASerde = new JsonSerde<>(TopicValueA.class);
        JsonSerde<TopicValueB> valueBSerde = new JsonSerde<>(TopicValueB.class);
        JsonSerde<JoinedResult> resultSerde = new JsonSerde<>(JoinedResult.class);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")
                .withZone(ZoneOffset.UTC);

        KStream<String, TopicMessageA> topicAStream = builder.stream(topicA, Consumed.with(Serdes.String(), wrapperASerde));
        KStream<String, TopicMessageB> topicBStream = builder.stream(topicB, Consumed.with(Serdes.String(), wrapperBSerde));

        // Log raw messages
        topicAStream.foreach((key, value) -> logger.info("Raw A: {} => {}", key, value));
        topicBStream.foreach((key, value) -> logger.info("Raw B: {} => {}", key, value));

        KStream<String, TopicValueA> filteredA = topicAStream
                .filter((key, msg) -> msg != null && msg.value != null && isValid(msg.value, formatter))
                .map((key, msg) -> KeyValue.pair(buildJoinKey(msg.value.catalog_number, msg.value.country), msg.value));

        KStream<String, TopicValueB> filteredB = topicBStream
                .filter((key, msg) -> msg != null && msg.value != null && isValid(msg.value, formatter))
                .map((key, msg) -> KeyValue.pair(buildJoinKey(msg.value.catalog_number, msg.value.country), msg.value));

        filteredA.foreach((key, value) -> logger.info("Filtered A: {} => {}", key, value));
        filteredB.foreach((key, value) -> logger.info("Filtered B: {} => {}", key, value));

        KTable<String, TopicValueA> tableA = filteredA.toTable(Materialized.with(Serdes.String(), valueASerde));
        KTable<String, TopicValueB> tableB = filteredB.toTable(Materialized.with(Serdes.String(), valueBSerde));

        // Join records from topic A and B
        KTable<String, JoinedResult> joined = tableA.join(tableB, (a, b) -> {
            if (a == null || b == null) return null;
            return new JoinedResult(a, b);
        }, Materialized.with(Serdes.String(), resultSerde));

        joined.suppress(Suppressed.untilTimeLimit(Duration.ofMillis(500), Suppressed.BufferConfig.unbounded()))
                .toStream()
                .peek((key, value) -> logger.info("Emitting final result: {} => {}", key, value))
                .to(topicC, Produced.with(Serdes.String(), resultSerde));

        return joined.toStream();
    }

    private String buildJoinKey(String catalog, String country) {
        return catalog + "-" + country;
    }

    private boolean isValid(TopicValueA value, DateTimeFormatter formatter) {
        System.out.println(value.catalog_number);
        return "001".equals(value.country)
                && value.catalog_number != null && value.catalog_number.length() == 5
                && isValidDate(value.selling_status_date, formatter);
    }

    private boolean isValid(TopicValueB value, DateTimeFormatter formatter) {
        return "001".equals(value.country)
                && value.catalog_number != null && value.catalog_number.length() == 5
                && isValidDate(value.sales_date, formatter);
    }

    private boolean isValidDate(String dateStr, DateTimeFormatter formatter) {
        try {
            formatter.parse(dateStr);
            return true;
        } catch (Exception e) {
            logger.warn("Invalid date: {}", dateStr, e);
            return false;
        }
    }
}
