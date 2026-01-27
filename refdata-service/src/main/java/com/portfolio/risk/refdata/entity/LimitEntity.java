package com.portfolio.risk.refdata.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;

@Entity
@Table(name = "limits")
public class LimitEntity {
    @Id
    @Column(name = "limit_id", nullable = false, length = 64)
    private String limitId;

    @Column(name = "portfolio_id", nullable = false, length = 64)
    private String portfolioId;

    @Column(name = "book_id", nullable = false, length = 64)
    private String bookId;

    @Column(name = "limit_type", nullable = false, length = 32)
    private String limitType;

    @Column(name = "threshold", nullable = false, precision = 18, scale = 4)
    private BigDecimal threshold;

    @Column(name = "currency", nullable = false, length = 8)
    private String currency;

    public String getLimitId() {
        return limitId;
    }

    public void setLimitId(String limitId) {
        this.limitId = limitId;
    }

    public String getPortfolioId() {
        return portfolioId;
    }

    public void setPortfolioId(String portfolioId) {
        this.portfolioId = portfolioId;
    }

    public String getBookId() {
        return bookId;
    }

    public void setBookId(String bookId) {
        this.bookId = bookId;
    }

    public String getLimitType() {
        return limitType;
    }

    public void setLimitType(String limitType) {
        this.limitType = limitType;
    }

    public BigDecimal getThreshold() {
        return threshold;
    }

    public void setThreshold(BigDecimal threshold) {
        this.threshold = threshold;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }
}
