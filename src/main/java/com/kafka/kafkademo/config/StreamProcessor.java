package com.kafka.kafkademo.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.kafkademo.model.ValueA;
import com.kafka.kafkademo.model.ValueB;
import com.kafka.kafkademo.model.JoinedResult;
import com.kafka.kafkademo.serializer.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Configuration
public class StreamProcessor {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(getClass());

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
        JsonSerde<ValueA> valueASerde = new JsonSerde<>(ValueA.class);
        JsonSerde<ValueB> valueBSerde = new JsonSerde<>(ValueB.class);
        JsonSerde<JoinedResult> resultSerde = new JsonSerde<>(JoinedResult.class);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'").withZone(ZoneOffset.UTC);

        KStream<String, ValueA> topicAStream = builder.stream(topicA, Consumed.with(Serdes.String(), valueASerde));
        KStream<String, ValueB> topicBStream = builder.stream(topicB, Consumed.with(Serdes.String(), valueBSerde));

        topicAStream.foreach((key, value) -> System.out.println("Raw A: " + key + " => " + value));
        topicBStream.foreach((key, value) -> System.out.println("Raw B: " + key + " => " + value));

        // Filtering with custom validation
        KStream<String, ValueA> filteredA = topicAStream
                .filter((key, value) -> isValidA(value, formatter));

        KStream<String, ValueB> filteredB = topicBStream
                .filter((key, value) -> isValidB(value, formatter));

        filteredA.foreach((key, value) -> System.out.println("Filtered A: " + key + " => " + value));
        filteredB.foreach((key, value) -> System.out.println("Filtered B: " + key + " => " + value));

        // Re-key and convert to tables
        KTable<String, ValueA> tableA = filteredA
                .selectKey((k, v) -> buildJoinKey(v.catalog_number, v.country))
                .toTable(Materialized.with(Serdes.String(), valueASerde));

        KTable<String, ValueB> tableB = filteredB
                .selectKey((k, v) -> buildJoinKey(v.catalog_number, v.country))
                .toTable(Materialized.with(Serdes.String(), valueBSerde));

        tableA.toStream().foreach((key, value) -> System.out.println("TableA: " + key + " => " + value));
        tableB.toStream().foreach((key, value) -> System.out.println("TableB: " + key + " => " + value));


        KTable<String, JoinedResult> joined = tableA.join(
                tableB,
                (a, b) -> {
                    if (a == null || b == null) return null;
                    JoinedResult result = new JoinedResult();
                    result.catalog_number = a.catalog_number;
                    result.country = a.country;
                    result.is_selling = a.is_selling;
                    result.model = a.model;
                    result.product_id = a.product_id;
                    result.registration_id = a.registration_id;
                    result.registration_number = a.registration_number;
                    result.selling_status_date = a.selling_status_date;
                    result.order_number = b.order_number;
                    result.quantity = b.quantity;
                    result.sales_date = b.sales_date;
                    return result;
                },
                Materialized.with(Serdes.String(), new JsonSerde<>(JoinedResult.class)) // 👈 This is the fix
        );


        // Suppress intermediate updates for 5000 milliseconds
        joined
                .suppress(Suppressed.untilTimeLimit(Duration.ofMillis(500), Suppressed.BufferConfig.unbounded()))
                .toStream()
                .peek((key, value) -> System.out.println("Emitting final result: " + value))
                .to(topicC, Produced.with(Serdes.String(), resultSerde));

        KStream<String, JoinedResult> output = joined.toStream();
        output.foreach((key, value) -> System.out.println("Sending to topicC: key=" + key + ", value=" + value));
        output.to(topicC, Produced.with(Serdes.String(), resultSerde));

        return output;
    }

    private boolean isValidA(ValueA value, DateTimeFormatter formatter) {
        return value != null &&
                "001".equals(value.country) &&
                isValidCatalog(value.catalog_number) &&
                isValidDate(value.selling_status_date, formatter);
    }

    private boolean isValidB(ValueB value, DateTimeFormatter formatter) {
        return value != null &&
                "001".equals(value.country) &&
                isValidCatalog(value.catalog_number) &&
                isValidDate(value.sales_date, formatter);
    }

    private boolean isValidCatalog(String catalogNumber) {
        return catalogNumber != null && catalogNumber.length() == 5;
    }

    private boolean isValidDate(String dateStr, DateTimeFormatter formatter) {
        try {
            formatter.parse(dateStr); // Just checks if it matches format
            return true;
        } catch (Exception e) {
            System.out.println("Invalid date format: " + dateStr);
            return false;
        }
    }

    private String buildJoinKey(String catalog, String country) {
        return catalog + "-" + country;
    }


}
