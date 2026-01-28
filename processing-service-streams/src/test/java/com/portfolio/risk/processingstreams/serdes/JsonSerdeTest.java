package com.portfolio.risk.processingstreams.serdes;

import com.portfolio.risk.common.dto.TradeEvent;
import com.portfolio.risk.processingstreams.model.*;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JsonSerdeTest {

    @Test
    void tradeEventRoundTrip() {
        TradeEvent event = new TradeEvent(
                UUID.randomUUID(),
                Instant.parse("2024-06-01T12:00:00Z"),
                "trade-gw",
                "1.0",
                "T-1",
                "P-1",
                "B-1",
                "INS-1",
                new BigDecimal("100"),
                new BigDecimal("25.5"),
                "BUY",
                "USD"
        );

        JsonSerde<TradeEvent> serde = new JsonSerde<>(TradeEvent.class);
        byte[] bytes = serde.serializer().serialize("trades", event);
        TradeEvent restored = serde.deserializer().deserialize("trades", bytes);

        assertEquals(event, restored);
    }

    @Test
    void modelRoundTrip() {
        InstrumentRefData refData = new InstrumentRefData("INS-1", "SYM", "Name", "EQUITY", "USD",
                "EQUITY", "TECH", "US", "ACTIVE");
        FxRate fxRate = new FxRate("EUR", "USD", new BigDecimal("1.08"), Instant.parse("2024-06-01T12:00:00Z"));
        LimitConfig limit = new LimitConfig(
                "L-1",
                "P-1",
                "B-1",
                "VAR",
                new BigDecimal("100000"),
                "USD",
                new BigDecimal("1000000"),
                new BigDecimal("0.10"),
                new BigDecimal("0.25"),
                new BigDecimal("0.15"),
                new BigDecimal("5000000"),
                Instant.now()
        );
        Position position = new Position(
                "P-1",
                "B-1",
                "INS-1",
                "USD",
                "USD",
                new BigDecimal("10"),
                new BigDecimal("100"),
                new BigDecimal("1000"),
                new BigDecimal("1000"),
                Instant.now(),
                Instant.now()
        );
        ExposureBucket exposure = new ExposureBucket(
                "P-1",
                "CURRENCY",
                "USD",
                new BigDecimal("1000"),
                new BigDecimal("1000"),
                1,
                Instant.now()
        );
        RiskSignalEvent signal = new RiskSignalEvent(
                "SIG-1",
                "P-1",
                "LIMIT",
                "sector",
                "TECH",
                new BigDecimal("100"),
                new BigDecimal("80"),
                "OPEN",
                "HIGH",
                Instant.now()
        );

        assertRoundTrip(refData, new JsonSerde<>(InstrumentRefData.class));
        assertRoundTrip(fxRate, new JsonSerde<>(FxRate.class));
        assertRoundTrip(limit, new JsonSerde<>(LimitConfig.class));
        assertRoundTrip(position, new JsonSerde<>(Position.class));
        assertRoundTrip(exposure, new JsonSerde<>(ExposureBucket.class));
        assertRoundTrip(signal, new JsonSerde<>(RiskSignalEvent.class));
    }

    private <T> void assertRoundTrip(T value, JsonSerde<T> serde) {
        byte[] bytes = serde.serializer().serialize("topic", value);
        T restored = serde.deserializer().deserialize("topic", bytes);
        assertEquals(value, restored);
    }
}
