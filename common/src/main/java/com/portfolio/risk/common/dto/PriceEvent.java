package com.portfolio.risk.common.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record PriceEvent(
        @NotNull UUID eventId,
        @NotNull Instant eventTime,
        @NotBlank String source,
        @NotBlank String schemaVersion,
        @NotBlank String instrumentId,
        @NotNull @Positive BigDecimal price,
        @NotBlank String currency
) {
}
