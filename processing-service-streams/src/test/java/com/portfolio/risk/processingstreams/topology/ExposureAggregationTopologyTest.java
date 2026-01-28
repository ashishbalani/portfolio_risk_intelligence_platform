package com.portfolio.risk.processingstreams.topology;

import com.portfolio.risk.common.dto.TradeEvent;
import com.portfolio.risk.processingstreams.config.StreamsProps;
import com.portfolio.risk.processingstreams.metrics.StreamMetrics;
import com.portfolio.risk.processingstreams.model.ExposureBucket;
import com.portfolio.risk.processingstreams.model.FxRate;
import com.portfolio.risk.processingstreams.model.InstrumentRefData;
import com.portfolio.risk.processingstreams.serdes.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.Test;

import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ExposureAggregationTopologyTest {

    @Test
    void aggregatesAcrossSectorsAndCurrencies() {
        StreamsBuilder builder = new StreamsBuilder();
        StreamsProps streamsProps = new StreamsProps();
        streamsProps.setSuppressOutputs(false);
        StreamMetrics metrics = new StreamMetrics(new io.micrometer.core.instrument.simple.SimpleMeterRegistry());
        Topology topology = new StreamsTopology(streamsProps, metrics).topology(builder);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "exposure-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        try {
            props.put(StreamsConfig.STATE_DIR_CONFIG, Files.createTempDirectory("kstreams-exposure").toFile().getAbsolutePath());
        } catch (Exception ex) {
            throw new UncheckedIOException(new java.io.IOException("Failed to create state dir", ex));
        }

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
            TestInputTopic<String, InstrumentRefData> refdata = driver.createInputTopic(
                    streamsProps.getTopics().getRefdata(),
                    Serdes.String().serializer(),
                    new JsonSerde<>(InstrumentRefData.class).serializer()
            );
            TestInputTopic<String, FxRate> fx = driver.createInputTopic(
                    streamsProps.getTopics().getFx(),
                    Serdes.String().serializer(),
                    new JsonSerde<>(FxRate.class).serializer()
            );
            TestInputTopic<String, TradeEvent> trades = driver.createInputTopic(
                    streamsProps.getTopics().getTrades(),
                    Serdes.String().serializer(),
                    new JsonSerde<>(TradeEvent.class).serializer()
            );
            TestOutputTopic<String, ExposureBucket> exposures = driver.createOutputTopic(
                    streamsProps.getTopics().getExposuresCurrent(),
                    Serdes.String().deserializer(),
                    new JsonSerde<>(ExposureBucket.class).deserializer()
            );

            refdata.pipeInput("INS-A", instrument("INS-A", "USD", "EQUITY", "TECH", "US"));
            refdata.pipeInput("INS-B", instrument("INS-B", "EUR", "EQUITY", "TECH", "DE"));
            refdata.pipeInput("INS-C", instrument("INS-C", "USD", "EQUITY", "FIN", "US"));

            fx.pipeInput("EUR/USD", new FxRate("EUR", "USD", new BigDecimal("1.10"), Instant.parse("2024-06-01T12:00:00Z")));

            trades.pipeInput(null, trade("T1", "INS-A", "BUY", new BigDecimal("10"), new BigDecimal("100"), "USD"));
            trades.pipeInput(null, trade("T2", "INS-B", "BUY", new BigDecimal("5"), new BigDecimal("200"), "EUR"));
            trades.pipeInput(null, trade("T3", "INS-C", "BUY", new BigDecimal("8"), new BigDecimal("50"), "USD"));

            Map<String, ExposureBucket> byKey = drain(exposures);

            ExposureBucket techBucket = byKey.get("P-1|sector|TECH");
            ExposureBucket finBucket = byKey.get("P-1|sector|FIN");
            ExposureBucket usdBucket = byKey.get("P-1|currency|USD");
            ExposureBucket eurBucket = byKey.get("P-1|currency|EUR");

            assertBigDecimalEquals(new BigDecimal("2100.0"), techBucket.grossNotionalBase());
            assertBigDecimalEquals(new BigDecimal("400.0"), finBucket.grossNotionalBase());
            assertBigDecimalEquals(new BigDecimal("1400.0"), usdBucket.grossNotionalBase());
            assertBigDecimalEquals(new BigDecimal("1100.0"), eurBucket.grossNotionalBase());
        }
    }

    private Map<String, ExposureBucket> drain(TestOutputTopic<String, ExposureBucket> topic) {
        Map<String, ExposureBucket> latest = new HashMap<>();
        while (!topic.isEmpty()) {
            var record = topic.readKeyValue();
            latest.put(record.key, record.value);
        }
        return latest;
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

    private InstrumentRefData instrument(String instrumentId, String ccy, String assetClass, String sector, String country) {
        return new InstrumentRefData(
                instrumentId,
                "SYM",
                "Name",
                "EQUITY",
                ccy,
                assetClass,
                sector,
                country,
                "ACTIVE"
        );
    }

    private void assertBigDecimalEquals(BigDecimal expected, BigDecimal actual) {
        assertTrue(expected.compareTo(actual) == 0, () -> "Expected " + expected + " but was " + actual);
    }
}
