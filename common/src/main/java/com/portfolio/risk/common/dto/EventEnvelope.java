package com.portfolio.risk.common.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

public record EventEnvelope<T>(
        @NotNull Headers headers,
        @Valid @NotNull T payload
) {
    public record Headers(
            String traceId,
            String tenantId
    ) {
    }
}
