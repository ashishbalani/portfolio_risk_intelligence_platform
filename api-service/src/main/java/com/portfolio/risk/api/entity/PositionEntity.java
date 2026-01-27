package com.portfolio.risk.api.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;

@Entity
@Table(name = "positions")
public class PositionEntity {
    @Id
    @Column(name = "position_id", nullable = false)
    private String positionId;

    @Column(name = "portfolio_id", nullable = false)
    private String portfolioId;

    @Column(name = "instrument_id", nullable = false)
    private String instrumentId;

    @Column(name = "quantity", nullable = false)
    private BigDecimal quantity;

    @Column(name = "market_value", nullable = false)
    private BigDecimal marketValue;

    @Column(name = "currency", nullable = false)
    private String currency;

    @Column(name = "as_of", nullable = false)
    private Instant asOf;

    public String getPositionId() {
        return positionId;
    }

    public void setPositionId(String positionId) {
        this.positionId = positionId;
    }

    public String getPortfolioId() {
        return portfolioId;
    }

    public void setPortfolioId(String portfolioId) {
        this.portfolioId = portfolioId;
    }

    public String getInstrumentId() {
        return instrumentId;
    }

    public void setInstrumentId(String instrumentId) {
        this.instrumentId = instrumentId;
    }

    public BigDecimal getQuantity() {
        return quantity;
    }

    public void setQuantity(BigDecimal quantity) {
        this.quantity = quantity;
    }

    public BigDecimal getMarketValue() {
        return marketValue;
    }

    public void setMarketValue(BigDecimal marketValue) {
        this.marketValue = marketValue;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public Instant getAsOf() {
        return asOf;
    }

    public void setAsOf(Instant asOf) {
        this.asOf = asOf;
    }
}
