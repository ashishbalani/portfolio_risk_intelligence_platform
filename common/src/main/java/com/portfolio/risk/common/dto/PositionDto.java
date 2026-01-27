package com.portfolio.risk.common.dto;

import java.math.BigDecimal;

public record PositionDto(
        String portfolioId,
        String instrumentId,
        BigDecimal quantity,
        BigDecimal marketValue,
        String currency
) {
}
