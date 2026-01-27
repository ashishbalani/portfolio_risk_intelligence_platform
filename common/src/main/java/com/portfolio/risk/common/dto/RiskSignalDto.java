package com.portfolio.risk.common.dto;

import java.math.BigDecimal;
import java.time.Instant;

public record RiskSignalDto(
        String signalId,
        String portfolioId,
        String type,
        BigDecimal value,
        Instant createdAt,
        String severity
) {
}
