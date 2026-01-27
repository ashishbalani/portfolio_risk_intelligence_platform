package com.portfolio.risk.common.error;

import java.time.Instant;
import java.util.Map;

public record ProblemDetails(
        String type,
        String title,
        int status,
        String detail,
        String instance,
        Instant timestamp,
        String eventId,
        Map<String, String> errors
) {
}
