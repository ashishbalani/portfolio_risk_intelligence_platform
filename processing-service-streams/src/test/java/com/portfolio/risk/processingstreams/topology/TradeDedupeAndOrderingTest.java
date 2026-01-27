package com.portfolio.risk.processingstreams.topology;

import com.portfolio.risk.common.dto.TradeEvent;
import com.portfolio.risk.processingstreams.config.StreamsProps;
import com.portfolio.risk.processingstreams.metrics.StreamMetrics;
import com.portfolio.risk.processingstreams.model.Position;
import com.portfolio.risk.processingstreams.serdes.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TradeDedupeAndOrderingTest {

    @Test
    void duplicateEventIdEmitsOnce() {
        StreamsProps props = new StreamsProps();
        props.setSuppressOutputs(false);
        StreamMetrics metrics = new StreamMetrics(new io.micrometer.core.instrument.simple.SimpleMeterRegistry());
        Topology topology = new StreamsTopology(props, metrics).topology(new StreamsBuilder());

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "dedupe-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {
            TestInputTopic<String, TradeEvent> trades = driver.createInputTopic(
                    props.getTopics().getTrades(),
                    Serdes.String().serializer(),
                    new JsonSerde<>(TradeEvent.class).serializer());
            TestOutputTopic<String, Position> positions = driver.createOutputTopic(
                    props.getTopics().getPositionsCurrent(),
                    Serdes.String().deserializer(),
                    new JsonSerde<>(Position.class).deserializer());

            UUID eventId = UUID.randomUUID();
            TradeEvent trade = new TradeEvent(
                    eventId,
                    Instant.parse("2024-06-01T12:00:00Z"),
                    "trade-gw",
                    "1.0",
                    "T-1",
                    "P-1",
                    "B-1",
                    "INS-1",
                    new BigDecimal("10"),
                    new BigDecimal("100"),
                    "BUY",
                    "USD"
            );

            trades.pipeInput(null, trade);
            trades.pipeInput(null, trade);

            assertEquals(1, positions.getQueueSize());
        }
    }

    @Test
    void outOfOrderOlderEventIgnored() {
        StreamsProps props = new StreamsProps();
        props.setSuppressOutputs(false);
        StreamMetrics metrics = new StreamMetrics(new io.micrometer.core.instrument.simple.SimpleMeterRegistry());
        Topology topology = new StreamsTopology(props, metrics).topology(new StreamsBuilder());

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {
            TestInputTopic<String, TradeEvent> trades = driver.createInputTopic(
                    props.getTopics().getTrades(),
                    Serdes.String().serializer(),
                    new JsonSerde<>(TradeEvent.class).serializer());
            TestOutputTopic<String, Position> positions = driver.createOutputTopic(
                    props.getTopics().getPositionsCurrent(),
                    Serdes.String().deserializer(),
                    new JsonSerde<>(Position.class).deserializer());

            trades.pipeInput(null, trade("T-1", Instant.parse("2024-06-01T12:00:10Z"), new BigDecimal("5")));
            trades.pipeInput(null, trade("T-2", Instant.parse("2024-06-01T12:00:00Z"), new BigDecimal("5")));

            Position first = positions.readValue();
            assertEquals(new BigDecimal("5"), first.netQty());
            assertEquals(0, positions.getQueueSize());
        }
    }

    private TradeEvent trade(String tradeId, Instant eventTime, BigDecimal qty) {
        return new TradeEvent(
                UUID.randomUUID(),
                eventTime,
                "trade-gw",
                "1.0",
                tradeId,
                "P-1",
                "B-1",
                "INS-1",
                qty,
                new BigDecimal("100"),
                "BUY",
                "USD"
        );
    }
}
