package com.portfolio.risk.processingstreams.metrics;

import org.apache.kafka.streams.KafkaStreams;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

@Component
public class StreamsStateHealthIndicator implements HealthIndicator {
    private final KafkaStreams streams;

    public StreamsStateHealthIndicator(KafkaStreams streams) {
        this.streams = streams;
    }

    @Override
    public Health health() {
        KafkaStreams.State state = streams.state();
        Health.Builder builder;
        if (state == KafkaStreams.State.RUNNING || state == KafkaStreams.State.REBALANCING) {
            builder = Health.up();
        } else if (state == KafkaStreams.State.ERROR) {
            builder = Health.down();
        } else {
            builder = Health.unknown();
        }
        return builder.withDetail("state", state.name()).build();
    }
}
