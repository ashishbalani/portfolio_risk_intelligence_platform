package com.portfolio.risk.processingstreams.model;

public record ExposureWithLimit(
        ExposureBucket exposure,
        LimitConfig limit
) {
}
