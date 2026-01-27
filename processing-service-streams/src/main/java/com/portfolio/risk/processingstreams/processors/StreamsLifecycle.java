package com.portfolio.risk.processingstreams.processors;

import org.apache.kafka.streams.KafkaStreams;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;

public class StreamsLifecycle implements SmartLifecycle {
    private final KafkaStreams streams;
    private volatile boolean running = false;

    public StreamsLifecycle(KafkaStreams streams) {
        this.streams = streams;
    }

    @Override
    public void start() {
        if (!running) {
            streams.start();
            running = true;
        }
    }

    @Override
    public void stop() {
        streams.close(Duration.ofSeconds(10));
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop(Runnable callback) {
        stop();
        callback.run();
    }
}
