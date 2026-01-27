package com.portfolio.risk.processingstreams.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.portfolio.risk.processingstreams.model.ErrorEnvelope;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;

public class StreamsDlqExceptionHandler implements DeserializationExceptionHandler {
    private static final Logger logger = LoggerFactory.getLogger(StreamsDlqExceptionHandler.class);
    private static final String DLQ_TOPIC_CONFIG = "processing.dlq.topic";
    private static final String INCLUDE_STACKTRACE = "processing.dlq.include-stacktrace";

    private KafkaProducer<String, ErrorEnvelope> producer;
    private String dlqTopic;
    private boolean includeStackTrace;

    @Override
    public DeserializationHandlerResponse handle(ProcessorContext context,
                                                 ConsumerRecord<byte[], byte[]> record,
                                                 Exception exception) {
        try {
            ErrorEnvelope envelope = new ErrorEnvelope(
                    record.topic(),
                    base64(record.key()),
                    base64(record.value()),
                    "DESERIALIZATION_ERROR",
                    exception.getMessage(),
                    includeStackTrace ? stackTrace(exception) : null,
                    Instant.now(),
                    headerValue(record, "X-Event-Id")
            );
            ProducerRecord<String, ErrorEnvelope> dlqRecord = new ProducerRecord<>(dlqTopic, envelope);
            producer.send(dlqRecord);
        } catch (Exception ex) {
            logger.error("Failed to send DLQ record", ex);
            return DeserializationHandlerResponse.FAIL;
        }
        return DeserializationHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        Object dlq = configs.get(DLQ_TOPIC_CONFIG);
        dlqTopic = dlq == null ? "trades.v1.dlq" : dlq.toString();
        Object include = configs.get(INCLUDE_STACKTRACE);
        includeStackTrace = Boolean.parseBoolean(include == null ? "false" : include.toString());

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        JsonSerializer<ErrorEnvelope> serializer = new JsonSerializer<>(mapper);
        serializer.setAddTypeInfo(false);

        Properties props = new Properties();
        Object bootstrap = configs.get("bootstrap.servers");
        if (bootstrap != null) {
            props.put("bootstrap.servers", bootstrap.toString());
        }
        this.producer = new KafkaProducer<>(props, new StringSerializer(), serializer);
    }

    public void close() {
        if (producer != null) {
            producer.close();
        }
    }

    private String base64(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        return Base64.getEncoder().encodeToString(bytes);
    }

    private String stackTrace(Exception exception) {
        StringBuilder builder = new StringBuilder();
        builder.append(exception).append("\n");
        for (StackTraceElement element : exception.getStackTrace()) {
            builder.append("\tat ").append(element).append("\n");
        }
        return builder.toString();
    }

    private String headerValue(ConsumerRecord<byte[], byte[]> record, String header) {
        if (record.headers() == null || record.headers().lastHeader(header) == null) {
            return null;
        }
        return new String(record.headers().lastHeader(header).value(), StandardCharsets.UTF_8);
    }
}
