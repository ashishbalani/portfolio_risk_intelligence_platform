package com.portfolio.risk.common;

import com.portfolio.risk.common.dto.TradeEvent;
import com.portfolio.risk.common.util.JacksonSupport;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventSerializationTest {

    @Test
    void tradeEventRoundTrip() throws Exception {
        TradeEvent event = new TradeEvent(
                UUID.randomUUID(),
                Instant.parse("2024-06-01T12:00:00Z"),
                "order-gateway",
                "1.0",
                "T-1",
                "P-ALPHA",
                "BOOK-ALPHA",
                "INS-IBM",
                new BigDecimal("100.5"),
                new BigDecimal("185.25"),
                "BUY",
                "USD"
        );

        var mapper = JacksonSupport.objectMapper();
        String json = mapper.writeValueAsString(event);
        TradeEvent restored = mapper.readValue(json, TradeEvent.class);

        assertEquals(event, restored);
        assertTrue(json.contains("\"eventTime\":\"2024-06-01T12:00:00Z\""));
    }
}
