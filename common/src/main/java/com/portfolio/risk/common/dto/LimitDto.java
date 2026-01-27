package com.portfolio.risk.common.dto;

import java.math.BigDecimal;

public record LimitDto(
        String limitId,
        String portfolioId,
        String limitType,
        BigDecimal threshold
) {
}
