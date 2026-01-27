package com.portfolio.risk.refdata.service;

import com.portfolio.risk.common.dto.RefDataInstrumentEvent;
import com.portfolio.risk.common.observability.EventIdGenerator;
import com.portfolio.risk.common.observability.EventKeys;
import com.portfolio.risk.refdata.entity.InstrumentEntity;
import com.portfolio.risk.refdata.repository.InstrumentRepository;
import jakarta.transaction.Transactional;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Service
public class InstrumentService {
    private final InstrumentRepository instrumentRepository;
    private final RefdataPublisher refdataPublisher;

    public InstrumentService(InstrumentRepository instrumentRepository, RefdataPublisher refdataPublisher) {
        this.instrumentRepository = instrumentRepository;
        this.refdataPublisher = refdataPublisher;
    }

    public List<InstrumentEntity> search(String query) {
        if (query == null || query.isBlank()) {
            return instrumentRepository.findAll();
        }
        return instrumentRepository.findBySymbolContainingIgnoreCaseOrNameContainingIgnoreCase(query, query);
    }

    public Optional<InstrumentEntity> get(String instrumentId) {
        return instrumentRepository.findById(instrumentId);
    }

    @Transactional
    public InstrumentEntity upsert(InstrumentEntity instrument) {
        InstrumentEntity saved = instrumentRepository.save(instrument);
        RefDataInstrumentEvent event = new RefDataInstrumentEvent(
                EventIdGenerator.nextEventId(),
                Instant.now(),
                "refdata-service",
                "1.0",
                saved.getInstrumentId(),
                saved.getSymbol(),
                saved.getType(),
                saved.getCurrency(),
                saved.getStatus()
        );
        String key = EventKeys.deterministicKey("", "", saved.getInstrumentId());
        refdataPublisher.publishInstrument(key, event.eventId().toString(), event);
        return saved;
    }
}
