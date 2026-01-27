package com.portfolio.risk.processing.service;

import com.portfolio.risk.common.dto.PositionDto;
import com.portfolio.risk.common.dto.PriceEvent;
import com.portfolio.risk.common.dto.TradeEvent;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class PositionService {
    private final Map<String, BigDecimal> quantities = new ConcurrentHashMap<>();
    private final Map<String, BigDecimal> prices = new ConcurrentHashMap<>();

    public PositionDto applyTrade(TradeEvent tradeEvent) {
        String key = key(tradeEvent.portfolioId(), tradeEvent.instrumentId());
        BigDecimal signedQty = "SELL".equalsIgnoreCase(tradeEvent.side())
                ? tradeEvent.quantity().negate()
                : tradeEvent.quantity();
        BigDecimal newQty = quantities.merge(key, signedQty, BigDecimal::add);
        BigDecimal price = prices.getOrDefault(tradeEvent.instrumentId(), tradeEvent.price());
        BigDecimal marketValue = price.multiply(newQty);
        return new PositionDto(tradeEvent.portfolioId(), tradeEvent.instrumentId(), newQty, marketValue, tradeEvent.currency());
    }

    public void applyPrice(PriceEvent priceEvent) {
        prices.put(priceEvent.instrumentId(), priceEvent.price());
    }

    private String key(String portfolioId, String instrumentId) {
        return portfolioId + "|" + instrumentId;
    }
}
