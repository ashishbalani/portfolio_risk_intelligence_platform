package com.portfolio.risk.processingstreams.topology;

import com.portfolio.risk.common.dto.TradeEvent;
import com.portfolio.risk.processingstreams.config.StreamsProps;
import com.portfolio.risk.processingstreams.metrics.StreamMetrics;
import com.portfolio.risk.processingstreams.model.FxRate;
import com.portfolio.risk.processingstreams.model.InstrumentRefData;
import com.portfolio.risk.processingstreams.model.LimitConfig;
import com.portfolio.risk.processingstreams.model.RiskSignalEvent;
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
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RiskSignalsTopologyTest {

    @Test
    void breachEmittedOnceAndDeduped() {
        StreamsProps streamsProps = new StreamsProps();
        streamsProps.setSuppressOutputs(false);
        StreamMetrics metrics = new StreamMetrics(new io.micrometer.core.instrument.simple.SimpleMeterRegistry());
        Topology topology = new StreamsTopology(streamsProps, metrics).topology(new StreamsBuilder());

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "risk-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
            TestInputTopic<String, InstrumentRefData> refdata = driver.createInputTopic(
                    streamsProps.getTopics().getRefdata(), Serdes.String().serializer(), new JsonSerde<>(InstrumentRefData.class).serializer());
            TestInputTopic<String, FxRate> fx = driver.createInputTopic(
                    streamsProps.getTopics().getFx(), Serdes.String().serializer(), new JsonSerde<>(FxRate.class).serializer());
            TestInputTopic<String, LimitConfig> limits = driver.createInputTopic(
                    streamsProps.getTopics().getLimits(), Serdes.String().serializer(), new JsonSerde<>(LimitConfig.class).serializer());
            TestInputTopic<String, TradeEvent> trades = driver.createInputTopic(
                    streamsProps.getTopics().getTrades(), Serdes.String().serializer(), new JsonSerde<>(TradeEvent.class).serializer());
            TestOutputTopic<String, RiskSignalEvent> signals = driver.createOutputTopic(
                    streamsProps.getTopics().getRiskSignals(), Serdes.String().deserializer(), new JsonSerde<>(RiskSignalEvent.class).deserializer());

            refdata.pipeInput("INS-1", instrument("INS-1", "USD", "TECH"));
            fx.pipeInput("USD/USD", new FxRate("USD", "USD", BigDecimal.ONE, Instant.parse("2024-06-01T12:00:00Z")));
            limits.pipeInput("P-1", limit("P-1", new BigDecimal("5000"), new BigDecimal("0.10")));

            trades.pipeInput(null, trade("T1", "INS-1", "BUY", new BigDecimal("10"), new BigDecimal("100"), "USD"));
            trades.pipeInput(null, trade("T2", "INS-1", "BUY", new BigDecimal("1"), new BigDecimal("100"), "USD"));

            List<RiskSignalEvent> emitted = signals.readValuesToList();
            assertEquals(1, emitted.size());
            assertEquals("OPEN", emitted.get(0).status());
        }
    }

    @Test
    void closeEmittedWhenBreachClears() {
        StreamsProps streamsProps = new StreamsProps();
        streamsProps.setSuppressOutputs(false);
        StreamMetrics metrics = new StreamMetrics(new io.micrometer.core.instrument.simple.SimpleMeterRegistry());
        Topology topology = new StreamsTopology(streamsProps, metrics).topology(new StreamsBuilder());

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "risk-test-close");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
            TestInputTopic<String, InstrumentRefData> refdata = driver.createInputTopic(
                    streamsProps.getTopics().getRefdata(), Serdes.String().serializer(), new JsonSerde<>(InstrumentRefData.class).serializer());
            TestInputTopic<String, LimitConfig> limits = driver.createInputTopic(
                    streamsProps.getTopics().getLimits(), Serdes.String().serializer(), new JsonSerde<>(LimitConfig.class).serializer());
            TestInputTopic<String, TradeEvent> trades = driver.createInputTopic(
                    streamsProps.getTopics().getTrades(), Serdes.String().serializer(), new JsonSerde<>(TradeEvent.class).serializer());
            TestOutputTopic<String, RiskSignalEvent> signals = driver.createOutputTopic(
                    streamsProps.getTopics().getRiskSignals(), Serdes.String().deserializer(), new JsonSerde<>(RiskSignalEvent.class).deserializer());

            refdata.pipeInput("INS-1", instrument("INS-1", "USD", "TECH"));
            limits.pipeInput("P-1", limit("P-1", new BigDecimal("5000"), new BigDecimal("0.10")));

            trades.pipeInput(null, trade("T1", "INS-1", "BUY", new BigDecimal("10"), new BigDecimal("100"), "USD"));
            trades.pipeInput(null, trade("T2", "INS-1", "SELL", new BigDecimal("5"), new BigDecimal("100"), "USD"));

            List<RiskSignalEvent> emitted = signals.readValuesToList();
            assertEquals(2, emitted.size());
            assertEquals("OPEN", emitted.get(0).status());
            assertEquals("CLOSED", emitted.get(1).status());
        }
    }

    private TradeEvent trade(String tradeId, String instrumentId, String side, BigDecimal qty, BigDecimal price, String ccy) {
        return new TradeEvent(
                UUID.randomUUID(),
                Instant.parse("2024-06-01T12:00:00Z"),
                "trade-gw",
                "1.0",
                tradeId,
                "P-1",
                "B-1",
                instrumentId,
                qty,
                price,
                side,
                ccy
        );
    }

    private InstrumentRefData instrument(String instrumentId, String ccy, String sector) {
        return new InstrumentRefData(
                instrumentId,
                "SYM",
                "Name",
                "EQUITY",
                ccy,
                "EQUITY",
                sector,
                "US",
                "ACTIVE"
        );
    }

    private LimitConfig limit(String portfolioId, BigDecimal nav, BigDecimal maxSingleNamePct) {
        return new LimitConfig(
                "L-1",
                portfolioId,
                "B-1",
                "VAR",
                new BigDecimal("0"),
                "USD",
                nav,
                maxSingleNamePct,
                null,
                null,
                null,
                Instant.parse("2024-06-01T12:00:00Z")
        );
    }
}
