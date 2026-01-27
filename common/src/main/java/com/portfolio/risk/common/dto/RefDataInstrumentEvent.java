package com.portfolio.risk.common.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;
import java.util.UUID;

public record RefDataInstrumentEvent(
        @NotNull UUID eventId,
        @NotNull Instant eventTime,
        @NotBlank String source,
        @NotBlank String schemaVersion,
        @NotBlank String instrumentId,
        @NotBlank String symbol,
        @NotBlank String type,
        @NotBlank String currency,
        @NotBlank String status
) {
}
