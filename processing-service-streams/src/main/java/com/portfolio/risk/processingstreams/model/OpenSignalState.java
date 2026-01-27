package com.portfolio.risk.processingstreams.model;

import java.time.Instant;

public record OpenSignalState(
        boolean open,
        Instant lastEmittedAt
) {
}
