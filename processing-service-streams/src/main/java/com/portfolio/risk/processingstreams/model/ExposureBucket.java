package com.portfolio.risk.processingstreams.model;

import java.math.BigDecimal;
import java.time.Instant;

public record ExposureBucket(
        String portfolioId,
        String dimension,
        String bucket,
        BigDecimal grossNotionalBase,
        BigDecimal netNotionalBase,
        int positionCount,
        Instant updatedAt
) {
}
