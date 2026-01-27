package com.portfolio.risk.refdata.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "portfolios")
public class PortfolioEntity {
    @Id
    @Column(name = "portfolio_id", nullable = false, length = 64)
    private String portfolioId;

    @Column(name = "name", nullable = false, length = 128)
    private String name;

    public String getPortfolioId() {
        return portfolioId;
    }

    public void setPortfolioId(String portfolioId) {
        this.portfolioId = portfolioId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
