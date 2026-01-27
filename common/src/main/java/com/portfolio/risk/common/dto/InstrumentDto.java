package com.portfolio.risk.common.dto;

public record InstrumentDto(
        String instrumentId,
        String symbol,
        String type,
        String currency
) {
}
