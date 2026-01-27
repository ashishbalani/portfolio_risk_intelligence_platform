package com.portfolio.risk.processingstreams.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public class StreamMetrics {
    private final MeterRegistry registry;
    private final ConcurrentMap<String, Counter> counters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Timer> timers = new ConcurrentHashMap<>();

    public StreamMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public void increment(String stage) {
        counters.computeIfAbsent(stage, key -> Counter.builder("streams.stage.throughput")
                .tag("stage", key)
                .register(registry))
                .increment();
    }

    public void recordLatency(String stage, Instant eventTime) {
        if (eventTime == null) {
            return;
        }
        Duration latency = Duration.between(eventTime, Instant.now());
        timers.computeIfAbsent(stage, key -> Timer.builder("streams.stage.latency")
                .tag("stage", key)
                .publishPercentiles(0.5, 0.9, 0.99)
                .register(registry))
                .record(latency);
    }
}
