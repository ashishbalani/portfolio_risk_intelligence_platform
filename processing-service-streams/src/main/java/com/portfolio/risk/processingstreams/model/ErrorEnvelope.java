package com.portfolio.risk.processingstreams.model;

import java.time.Instant;

public record ErrorEnvelope(
        String originalTopic,
        String originalKey,
        String rawPayload,
        String errorType,
        String message,
        String stackTrace,
        Instant failedAt,
        String correlationId
) {
}
