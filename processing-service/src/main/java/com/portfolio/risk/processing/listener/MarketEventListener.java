package com.portfolio.risk.processing.listener;

import com.portfolio.risk.common.dto.PriceEvent;
import com.portfolio.risk.common.dto.PositionDto;
import com.portfolio.risk.common.dto.TradeEvent;
import com.portfolio.risk.common.observability.EventIdGenerator;
import com.portfolio.risk.common.observability.EventIdHeader;
import com.portfolio.risk.processing.service.PositionService;
import com.portfolio.risk.processing.service.RiskSignalPublisher;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public class MarketEventListener {
    private final PositionService positionService;
    private final RiskSignalPublisher riskSignalPublisher;

    public MarketEventListener(PositionService positionService, RiskSignalPublisher riskSignalPublisher) {
        this.positionService = positionService;
        this.riskSignalPublisher = riskSignalPublisher;
    }

    @KafkaListener(topics = "trades", groupId = "processing-service")
    public void onTrade(ConsumerRecord<String, TradeEvent> record) {
        String eventId = resolveEventId(record.headers());
        MDC.put(EventIdHeader.MDC_KEY, eventId);
        try {
            PositionDto position = positionService.applyTrade(record.value());
            riskSignalPublisher.publishSignal(position, eventId);
        } finally {
            MDC.remove(EventIdHeader.MDC_KEY);
        }
    }

    @KafkaListener(topics = "prices", groupId = "processing-service")
    public void onPrice(ConsumerRecord<String, PriceEvent> record) {
        String eventId = resolveEventId(record.headers());
        MDC.put(EventIdHeader.MDC_KEY, eventId);
        try {
            positionService.applyPrice(record.value());
        } finally {
            MDC.remove(EventIdHeader.MDC_KEY);
        }
    }

    private String resolveEventId(Headers headers) {
        Header header = headers.lastHeader(EventIdHeader.HEADER_NAME);
        if (header == null || header.value() == null || header.value().length == 0) {
            return EventIdGenerator.nextEventIdString();
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
