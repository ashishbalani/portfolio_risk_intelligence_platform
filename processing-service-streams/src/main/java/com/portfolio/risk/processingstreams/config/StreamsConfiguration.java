package com.portfolio.risk.processingstreams.config;

import com.portfolio.risk.processingstreams.processors.StreamsLifecycle;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class StreamsConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(StreamsConfiguration.class);

    @Bean
    public KafkaStreams kafkaStreams(org.apache.kafka.streams.Topology topology,
                                     StreamsProps streamsProps,
                                     Environment env) {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, streamsProps.getBootstrapServers());
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, streamsProps.getApplicationId());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, streamsProps.getNumStreamThreads());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, streamsProps.getCommitIntervalMs());
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, streamsProps.getCacheMaxBytesBuffering());
        props.put(StreamsConfig.STATE_DIR_CONFIG, streamsProps.getStateDir());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, streamsProps.getDefaultKeySerde());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, streamsProps.getDefaultValueSerde());
        if (streamsProps.isExactlyOnce()) {
            props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        }
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, StreamsDlqExceptionHandler.class);
        props.put("processing.dlq.topic", env.getProperty("processing.dlq.topic", "trades.v1.dlq"));
        props.put("processing.dlq.include-stacktrace", env.getProperty("processing.dlq.include-stacktrace", "false"));

        KafkaStreams streams = new KafkaStreams(topology, new StreamsConfig(props));
        streams.setUncaughtExceptionHandler(exception -> {
            logger.error("Kafka Streams uncaught exception", exception);
            if (exception instanceof ConfigException) {
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
            }
            return streamsProps.isReplaceThreadOnException()
                    ? StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD
                    : StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
        });
        return streams;
    }

    @Bean
    public StreamsLifecycle streamsLifecycle(KafkaStreams kafkaStreams) {
        return new StreamsLifecycle(kafkaStreams);
    }
}
