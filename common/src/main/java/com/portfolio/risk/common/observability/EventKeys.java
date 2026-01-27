package com.portfolio.risk.common.observability;

public final class EventKeys {
    private EventKeys() {
    }

    public static String deterministicKey(String portfolioId, String bookId, String instrumentId) {
        String safePortfolio = portfolioId == null ? "" : portfolioId;
        String safeBook = bookId == null ? "" : bookId;
        String safeInstrument = instrumentId == null ? "" : instrumentId;
        return safePortfolio + "|" + safeBook + "|" + safeInstrument;
    }
}
