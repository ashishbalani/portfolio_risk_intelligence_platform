package com.portfolio.risk.common.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record FxRateEvent(
        @NotNull UUID eventId,
        @NotNull Instant eventTime,
        @NotBlank String source,
        @NotBlank String schemaVersion,
        @NotBlank String baseCurrency,
        @NotBlank String quoteCurrency,
        @NotNull @Positive BigDecimal rate
) {
}
