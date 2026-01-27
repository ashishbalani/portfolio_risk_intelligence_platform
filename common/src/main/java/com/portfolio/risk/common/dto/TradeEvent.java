package com.portfolio.risk.common.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record TradeEvent(
        @NotNull UUID eventId,
        @NotNull Instant eventTime,
        @NotBlank String source,
        @NotBlank String schemaVersion,
        @NotBlank String tradeId,
        @NotBlank String portfolioId,
        @NotBlank String bookId,
        @NotBlank String instrumentId,
        @NotNull @Positive BigDecimal quantity,
        @NotNull @Positive BigDecimal price,
        @NotBlank String side,
        @NotBlank String currency
) {
}
