package com.portfolio.risk.processingstreams.model;

import java.math.BigDecimal;
import java.time.Instant;

public record Position(
        String portfolioId,
        String bookId,
        String instrumentId,
        String instrumentCcy,
        String baseCcy,
        BigDecimal netQty,
        BigDecimal avgPrice,
        BigDecimal grossNotionalBase,
        BigDecimal netNotionalBase,
        Instant lastEventTime,
        Instant lastUpdateTime
) {
}
