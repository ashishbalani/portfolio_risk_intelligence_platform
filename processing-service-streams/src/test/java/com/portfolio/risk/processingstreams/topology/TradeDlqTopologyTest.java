package com.portfolio.risk.processingstreams.topology;

import com.portfolio.risk.common.dto.TradeEvent;
import com.portfolio.risk.processingstreams.config.StreamsProps;
import com.portfolio.risk.processingstreams.metrics.StreamMetrics;
import com.portfolio.risk.processingstreams.model.ErrorEnvelope;
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

class TradeDlqTopologyTest {

    @Test
    void invalidTradeGoesToDlq() {
        StreamsProps streamsProps = new StreamsProps();
        streamsProps.setSuppressOutputs(false);
        StreamMetrics metrics = new StreamMetrics(new io.micrometer.core.instrument.simple.SimpleMeterRegistry());
        Topology topology = new StreamsTopology(streamsProps, metrics).topology(new StreamsBuilder());

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "dlq-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
            TestInputTopic<String, TradeEvent> trades = driver.createInputTopic(
                    streamsProps.getTopics().getTrades(), Serdes.String().serializer(), new JsonSerde<>(TradeEvent.class).serializer());
            TestOutputTopic<String, ErrorEnvelope> dlq = driver.createOutputTopic(
                    streamsProps.getTopics().getTradesDlq(), Serdes.String().deserializer(), new JsonSerde<>(ErrorEnvelope.class).deserializer());

            TradeEvent invalid = new TradeEvent(
                    UUID.randomUUID(),
                    Instant.parse("2024-06-01T12:00:00Z"),
                    "trade-gw",
                    "1.0",
                    "T-1",
                    "P-1",
                    "B-1",
                    "",
                    new BigDecimal("10"),
                    new BigDecimal("100"),
                    "BUY",
                    "USD"
            );

            trades.pipeInput(null, invalid);

            ErrorEnvelope envelope = dlq.readValue();
            assertEquals("VALIDATION_ERROR", envelope.errorType());
            assertEquals("T-1", envelope.originalKey());
        }
    }
}
