package com.portfolio.risk.processingstreams.model;

import java.math.BigDecimal;
import java.time.Instant;

public record FxRate(
        String baseCurrency,
        String quoteCurrency,
        BigDecimal rate,
        Instant asOf
) {
}
