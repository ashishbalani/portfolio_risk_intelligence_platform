package com.portfolio.risk.refdata.service;

import com.portfolio.risk.common.dto.LimitConfigEvent;
import com.portfolio.risk.common.dto.RefDataInstrumentEvent;
import com.portfolio.risk.common.observability.EventIdHeader;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public class RefdataPublisher {
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public RefdataPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishInstrument(String key, String eventId, RefDataInstrumentEvent event) {
        ProducerRecord<String, Object> record = new ProducerRecord<>("refdata.v1", key, event);
        record.headers().add(EventIdHeader.HEADER_NAME, eventId.getBytes(StandardCharsets.UTF_8));
        kafkaTemplate.send(record);
    }

    public void publishLimit(String key, String eventId, LimitConfigEvent event) {
        ProducerRecord<String, Object> record = new ProducerRecord<>("limits.v1", key, event);
        record.headers().add(EventIdHeader.HEADER_NAME, eventId.getBytes(StandardCharsets.UTF_8));
        kafkaTemplate.send(record);
    }
}
