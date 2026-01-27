package com.portfolio.risk.processingstreams.model;

import java.math.BigDecimal;

public record ExposureDelta(
        String portfolioId,
        String dimension,
        String bucket,
        BigDecimal grossNotionalDelta,
        BigDecimal netNotionalDelta,
        int positionDelta
) {
}
