package com.portfolio.risk.processingstreams.model;

import java.math.BigDecimal;
import java.time.Instant;

public record LimitConfig(
        String limitId,
        String portfolioId,
        String bookId,
        String limitType,
        BigDecimal threshold,
        String currency,
        BigDecimal nav,
        BigDecimal maxSingleNamePct,
        BigDecimal maxSectorPct,
        BigDecimal maxFxPct,
        BigDecimal grossNotionalCap,
        Instant asOf
) {
}
