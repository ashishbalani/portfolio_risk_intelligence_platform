package com.portfolio.risk.api.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;

@Entity
@Table(name = "risk_signals")
public class RiskSignalEntity {
    @Id
    @Column(name = "signal_id", nullable = false)
    private String signalId;

    @Column(name = "portfolio_id", nullable = false)
    private String portfolioId;

    @Column(name = "signal_type", nullable = false)
    private String type;

    @Column(name = "signal_value", nullable = false)
    private BigDecimal value;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "severity", nullable = false)
    private String severity;

    public String getSignalId() {
        return signalId;
    }

    public void setSignalId(String signalId) {
        this.signalId = signalId;
    }

    public String getPortfolioId() {
        return portfolioId;
    }

    public void setPortfolioId(String portfolioId) {
        this.portfolioId = portfolioId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public BigDecimal getValue() {
        return value;
    }

    public void setValue(BigDecimal value) {
        this.value = value;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public String getSeverity() {
        return severity;
    }

    public void setSeverity(String severity) {
        this.severity = severity;
    }
}
