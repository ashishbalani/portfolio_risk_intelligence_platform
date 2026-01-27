package com.portfolio.risk.common;

import com.portfolio.risk.common.dto.TradeEvent;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventValidationTest {

    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    @Test
    void tradeEventValidatesRequiredFields() {
        TradeEvent valid = new TradeEvent(
                UUID.randomUUID(),
                Instant.now(),
                "order-gateway",
                "1.0",
                "T-1",
                "P-ALPHA",
                "BOOK-ALPHA",
                "INS-IBM",
                new BigDecimal("100"),
                new BigDecimal("185.25"),
                "BUY",
                "USD"
        );

        assertTrue(validator.validate(valid).isEmpty());

        TradeEvent invalid = new TradeEvent(
                null,
                null,
                " ",
                "",
                "",
                "",
                "",
                "",
                BigDecimal.ZERO,
                new BigDecimal("-1"),
                "",
                ""
        );

        assertFalse(validator.validate(invalid).isEmpty());
    }
}
