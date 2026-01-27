package com.portfolio.risk.processingstreams.model;

public record InstrumentRefData(
        String instrumentId,
        String symbol,
        String name,
        String type,
        String currency,
        String assetClass,
        String sector,
        String country,
        String status
) {
}
