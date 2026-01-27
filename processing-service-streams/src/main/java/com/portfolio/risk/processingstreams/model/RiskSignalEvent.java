package com.portfolio.risk.processingstreams.model;

import java.math.BigDecimal;
import java.time.Instant;

public record RiskSignalEvent(
        String signalId,
        String portfolioId,
        String type,
        String dimension,
        String bucket,
        BigDecimal value,
        BigDecimal threshold,
        String status,
        String severity,
        Instant createdAt
) {
}
