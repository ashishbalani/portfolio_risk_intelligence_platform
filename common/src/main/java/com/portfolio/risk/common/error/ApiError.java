package com.portfolio.risk.common.error;

import java.time.Instant;

public record ApiError(
        String message,
        ErrorCode code,
        Instant timestamp,
        String eventId
) {
}
