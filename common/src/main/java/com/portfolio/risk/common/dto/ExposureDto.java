package com.portfolio.risk.common.dto;

import java.math.BigDecimal;

public record ExposureDto(
        String portfolioId,
        String riskFactor,
        BigDecimal exposure
) {
}
