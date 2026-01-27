package com.portfolio.risk.common.observability;

import java.util.UUID;

public final class EventIdGenerator {
    private EventIdGenerator() {
    }

    public static UUID nextEventId() {
        return UUID.randomUUID();
    }

    public static String nextEventIdString() {
        return nextEventId().toString();
    }
}
