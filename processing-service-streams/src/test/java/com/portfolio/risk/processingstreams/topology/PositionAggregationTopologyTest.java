package com.portfolio.risk.processingstreams.topology;

import com.portfolio.risk.common.dto.TradeEvent;
import com.portfolio.risk.processingstreams.config.StreamsProps;
import com.portfolio.risk.processingstreams.metrics.StreamMetrics;
import com.portfolio.risk.processingstreams.model.FxRate;
import com.portfolio.risk.processingstreams.model.InstrumentRefData;
import com.portfolio.risk.processingstreams.model.Position;
import com.portfolio.risk.processingstreams.serdes.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.Test;

import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PositionAggregationTopologyTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, TradeEvent> trades;
    private TestInputTopic<String, InstrumentRefData> refdata;
    private TestInputTopic<String, FxRate> fx;
    private TestOutputTopic<String, Position> positions;

    private void setup() {
        StreamsBuilder builder = new StreamsBuilder();
        StreamsProps streamsProps = new StreamsProps();
        streamsProps.setSuppressOutputs(false);
        StreamMetrics metrics = new StreamMetrics(new io.micrometer.core.instrument.simple.SimpleMeterRegistry());
        Topology topology = new StreamsTopology(streamsProps, metrics).topology(builder);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        try {
            props.put(StreamsConfig.STATE_DIR_CONFIG, Files.createTempDirectory("kstreams-position").toFile().getAbsolutePath());
        } catch (Exception ex) {
            throw new UncheckedIOException(new java.io.IOException("Failed to create state dir", ex));
        }

        driver = new TopologyTestDriver(topology, props);
        trades = driver.createInputTopic(streamsProps.getTopics().getTrades(), Serdes.String().serializer(), new JsonSerde<>(TradeEvent.class).serializer());
        refdata = driver.createInputTopic(streamsProps.getTopics().getRefdata(), Serdes.String().serializer(), new JsonSerde<>(InstrumentRefData.class).serializer());
        fx = driver.createInputTopic(streamsProps.getTopics().getFx(), Serdes.String().serializer(), new JsonSerde<>(FxRate.class).serializer());
        positions = driver.createOutputTopic(streamsProps.getTopics().getPositionsCurrent(), Serdes.String().deserializer(), new JsonSerde<>(Position.class).deserializer());
    }

    @Test
    void multipleBuysWeightedAvg() {
        setup();
        refdata.pipeInput("INS-1", refdataEvent("INS-1", "USD"));

        trades.pipeInput(null, trade("T1", "INS-1", "BUY", new BigDecimal("10"), new BigDecimal("100"), "USD"));
        trades.pipeInput(null, trade("T2", "INS-1", "BUY", new BigDecimal("5"), new BigDecimal("120"), "USD"));

        positions.readValue();
        Position latest = positions.readValue();

        assertEquals(new BigDecimal("15"), latest.netQty());
        assertBigDecimalEquals(new BigDecimal("106.66666667"), latest.avgPrice());
        assertBigDecimalEquals(new BigDecimal("1600.00000005"), latest.grossNotionalBase());
        assertBigDecimalEquals(new BigDecimal("1600.00000005"), latest.netNotionalBase());
        teardown();
    }

    @Test
    void partialSellKeepsAvg() {
        setup();
        refdata.pipeInput("INS-1", refdataEvent("INS-1", "USD"));
        trades.pipeInput(null, trade("T1", "INS-1", "BUY", new BigDecimal("10"), new BigDecimal("100"), "USD"));
        trades.pipeInput(null, trade("T2", "INS-1", "SELL", new BigDecimal("4"), new BigDecimal("110"), "USD"));

        positions.readValue();
        Position latest = positions.readValue();
        assertEquals(new BigDecimal("6"), latest.netQty());
        assertBigDecimalEquals(new BigDecimal("100"), latest.avgPrice());
        teardown();
    }

    @Test
    void flipLongToShort() {
        setup();
        refdata.pipeInput("INS-1", refdataEvent("INS-1", "USD"));
        trades.pipeInput(null, trade("T1", "INS-1", "BUY", new BigDecimal("5"), new BigDecimal("100"), "USD"));
        trades.pipeInput(null, trade("T2", "INS-1", "SELL", new BigDecimal("8"), new BigDecimal("90"), "USD"));

        positions.readValue();
        Position latest = positions.readValue();
        assertEquals(new BigDecimal("-3"), latest.netQty());
        assertBigDecimalEquals(new BigDecimal("90"), latest.avgPrice());
        teardown();
    }

    @Test
    void missingFxOrRefdataHandled() {
        setup();
        trades.pipeInput(null, trade("T1", "INS-1", "BUY", new BigDecimal("10"), new BigDecimal("100"), "EUR"));

        Position latest = positions.readValue();
        assertEquals("EUR", latest.instrumentCcy());
        assertEquals("USD", latest.baseCcy());
        assertBigDecimalEquals(BigDecimal.ZERO, latest.netNotionalBase());
        teardown();
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

    private InstrumentRefData refdataEvent(String instrumentId, String ccy) {
        return new InstrumentRefData(
                instrumentId,
                "SYM",
                "Name",
                "EQUITY",
                ccy,
                "EQUITY",
                "TECH",
                "US",
                "ACTIVE"
        );
    }

    @SuppressWarnings("unused")
    private FxRate fxRate(String base, String quote, BigDecimal rate) {
        return new FxRate(base, quote, rate, Instant.parse("2024-06-01T12:00:00Z"));
    }

    private void teardown() {
        if (driver != null) {
            driver.close();
        }
    }

    private void assertBigDecimalEquals(BigDecimal expected, BigDecimal actual) {
        assertTrue(expected.compareTo(actual) == 0, () -> "Expected " + expected + " but was " + actual);
    }
}
