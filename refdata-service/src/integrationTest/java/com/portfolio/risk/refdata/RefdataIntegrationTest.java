package com.portfolio.risk.refdata;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.*;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class RefdataIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("refdata")
            .withUsername("refdata")
            .withPassword("refdata");

    @Container
    static final KafkaContainer kafka = new KafkaContainer("confluentinc/cp-kafka:7.6.1");

    @DynamicPropertySource
    static void registerProps(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("refdata.api-key", () -> "test-key");
    }

    @LocalServerPort
    int port;

    private TestRestTemplate restTemplate = new TestRestTemplate();

    private static KafkaConsumer<String, String> consumer;

    @BeforeAll
    static void setupConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "refdata-it");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(java.util.List.of("refdata.v1", "limits.v1"));
    }

    @AfterAll
    static void teardown() {
        if (consumer != null) {
            consumer.close(Duration.ofSeconds(1));
        }
    }

    @Test
    void instrumentUpsertPersistsAndPublishes() {
        String url = "http://localhost:" + port + "/instruments";
        Map<String, Object> payload = Map.of(
                "instrumentId", "INS-TEST",
                "symbol", "TST",
                "name", "Test Instrument",
                "type", "EQUITY",
                "currency", "USD",
                "status", "ACTIVE"
        );

        ResponseEntity<String> response = restTemplate.exchange(
                url,
                HttpMethod.POST,
                new HttpEntity<>(payload, headers()),
                String.class
        );
        assertEquals(200, response.getStatusCodeValue());

        ConsumerRecord<String, String> record = pollFor("refdata.v1");
        assertTrue(record.value().contains("\"instrumentId\":\"INS-TEST\""));
    }

    @Test
    void limitUpsertPersistsAndPublishes() {
        String url = "http://localhost:" + port + "/limits";
        Map<String, Object> payload = Map.of(
                "limitId", "LIM-1",
                "portfolioId", "P-ALPHA",
                "bookId", "BOOK-ALPHA",
                "limitType", "VAR",
                "threshold", 1000000,
                "currency", "USD"
        );

        ResponseEntity<String> response = restTemplate.exchange(
                url,
                HttpMethod.POST,
                new HttpEntity<>(payload, headers()),
                String.class
        );
        assertEquals(200, response.getStatusCodeValue());

        ConsumerRecord<String, String> record = pollFor("limits.v1");
        assertTrue(record.value().contains("\"limitId\":\"LIM-1\""));
    }

    private ConsumerRecord<String, String> pollFor(String topic) {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> record : records) {
                if (topic.equals(record.topic())) {
                    return record;
                }
            }
        }
        throw new IllegalStateException("No Kafka message found for " + topic);
    }

    private HttpHeaders headers() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.add("X-API-Key", "test-key");
        return headers;
    }
}
