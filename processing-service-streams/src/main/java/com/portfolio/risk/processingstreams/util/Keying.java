package com.portfolio.risk.processingstreams.util;

public final class Keying {
    private Keying() {
    }

    public static String positionKey(String portfolioId, String bookId, String instrumentId) {
        return safe(portfolioId) + "|" + safe(bookId) + "|" + safe(instrumentId);
    }

    public static String exposureKey(String portfolioId, String dimension, String bucket) {
        return safe(portfolioId) + "|" + safe(dimension) + "|" + safe(bucket);
    }

    private static String safe(String value) {
        return value == null ? "" : value;
    }
}
