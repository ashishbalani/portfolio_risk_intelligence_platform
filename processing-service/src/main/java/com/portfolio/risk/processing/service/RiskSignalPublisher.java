package com.portfolio.risk.processing.service;

import com.portfolio.risk.common.dto.PositionDto;
import com.portfolio.risk.common.dto.RiskSignalDto;
import com.portfolio.risk.common.observability.EventIdGenerator;
import com.portfolio.risk.common.observability.EventIdHeader;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Instant;

@Service
public class RiskSignalPublisher {
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public RiskSignalPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishSignal(PositionDto positionDto, String eventId) {
        String signalId = "SIG-" + EventIdGenerator.nextEventIdString();
        BigDecimal absValue = positionDto.marketValue().abs();
        String severity = absValue.compareTo(new BigDecimal("1000000")) > 0 ? "HIGH" : "NORMAL";
        RiskSignalDto signal = new RiskSignalDto(
                signalId,
                positionDto.portfolioId(),
                "POSITION_LIMIT",
                absValue,
                Instant.now(),
                severity
        );
        ProducerRecord<String, Object> record = new ProducerRecord<>("risk.signals", positionDto.portfolioId(), signal);
        record.headers().add(EventIdHeader.HEADER_NAME, (eventId == null ? "" : eventId).getBytes());
        kafkaTemplate.send(record);
    }
}
