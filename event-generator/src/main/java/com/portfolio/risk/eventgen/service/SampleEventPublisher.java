package com.portfolio.risk.eventgen.service;

import com.portfolio.risk.common.dto.FxRateEvent;
import com.portfolio.risk.common.dto.PriceEvent;
import com.portfolio.risk.common.dto.TradeEvent;
import com.portfolio.risk.common.observability.EventIdGenerator;
import com.portfolio.risk.common.observability.EventIdHeader;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.MDC;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

@Component
public class SampleEventPublisher implements ApplicationRunner {
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public SampleEventPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void run(ApplicationArguments args) {
        String source = "event-generator";
        String schemaVersion = "1.0";

        List<TradeEvent> trades = List.of(
                new TradeEvent(EventIdGenerator.nextEventId(), Instant.now(), source, schemaVersion,
                        "T-1001", "P-ALPHA", "BOOK-ALPHA", "INS-IBM", new BigDecimal("150"), new BigDecimal("185.50"), "BUY", "USD"),
                new TradeEvent(EventIdGenerator.nextEventId(), Instant.now(), source, schemaVersion,
                        "T-1002", "P-BETA", "BOOK-BETA", "INS-AAPL", new BigDecimal("75"), new BigDecimal("210.20"), "SELL", "USD")
        );

        List<PriceEvent> prices = List.of(
                new PriceEvent(EventIdGenerator.nextEventId(), Instant.now(), source, schemaVersion,
                        "INS-IBM", new BigDecimal("186.10"), "USD"),
                new PriceEvent(EventIdGenerator.nextEventId(), Instant.now(), source, schemaVersion,
                        "INS-AAPL", new BigDecimal("209.80"), "USD")
        );

        List<FxRateEvent> fxRates = List.of(
                new FxRateEvent(EventIdGenerator.nextEventId(), Instant.now(), source, schemaVersion,
                        "EUR", "USD", new BigDecimal("1.08")),
                new FxRateEvent(EventIdGenerator.nextEventId(), Instant.now(), source, schemaVersion,
                        "GBP", "USD", new BigDecimal("1.28"))
        );

        trades.forEach(event -> sendWithEventId("trades", event.tradeId(), event.eventId().toString(), event));
        prices.forEach(event -> sendWithEventId("prices", event.instrumentId(), event.eventId().toString(), event));
        fxRates.forEach(event -> sendWithEventId("fx-rates", event.baseCurrency(), event.eventId().toString(), event));
    }

    private void sendWithEventId(String topic, String key, String eventId, Object payload) {
        MDC.put(EventIdHeader.MDC_KEY, eventId);
        try {
            ProducerRecord<String, Object> record = new ProducerRecord<>(topic, key, payload);
            record.headers().add(EventIdHeader.HEADER_NAME, eventId.getBytes());
            kafkaTemplate.send(record);
        } finally {
            MDC.remove(EventIdHeader.MDC_KEY);
        }
    }
}
