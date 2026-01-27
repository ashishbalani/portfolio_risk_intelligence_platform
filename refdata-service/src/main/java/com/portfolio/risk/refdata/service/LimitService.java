package com.portfolio.risk.refdata.service;

import com.portfolio.risk.common.dto.LimitConfigEvent;
import com.portfolio.risk.common.observability.EventIdGenerator;
import com.portfolio.risk.common.observability.EventKeys;
import com.portfolio.risk.refdata.entity.LimitEntity;
import com.portfolio.risk.refdata.repository.LimitRepository;
import jakarta.transaction.Transactional;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

@Service
public class LimitService {
    private final LimitRepository limitRepository;
    private final RefdataPublisher refdataPublisher;

    public LimitService(LimitRepository limitRepository, RefdataPublisher refdataPublisher) {
        this.limitRepository = limitRepository;
        this.refdataPublisher = refdataPublisher;
    }

    public List<LimitEntity> findByPortfolio(String portfolioId) {
        if (portfolioId == null || portfolioId.isBlank()) {
            return limitRepository.findAll();
        }
        return limitRepository.findByPortfolioId(portfolioId);
    }

    @Transactional
    public LimitEntity upsert(LimitEntity limit) {
        LimitEntity saved = limitRepository.save(limit);
        LimitConfigEvent event = new LimitConfigEvent(
                EventIdGenerator.nextEventId(),
                Instant.now(),
                "refdata-service",
                "1.0",
                saved.getLimitId(),
                saved.getPortfolioId(),
                saved.getBookId(),
                saved.getLimitType(),
                saved.getThreshold(),
                saved.getCurrency()
        );
        String key = EventKeys.deterministicKey(saved.getPortfolioId(), saved.getBookId(), "");
        refdataPublisher.publishLimit(key, event.eventId().toString(), event);
        return saved;
    }
}
