package com.portfolio.risk.common.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record LimitConfigEvent(
        @NotNull UUID eventId,
        @NotNull Instant eventTime,
        @NotBlank String source,
        @NotBlank String schemaVersion,
        @NotBlank String limitId,
        @NotBlank String portfolioId,
        @NotBlank String bookId,
        @NotBlank String limitType,
        @NotNull @PositiveOrZero BigDecimal threshold,
        @NotBlank String currency
) {
}
