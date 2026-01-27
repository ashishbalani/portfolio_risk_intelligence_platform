package com.portfolio.risk.api.repository;

import com.portfolio.risk.api.entity.RiskSignalEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface RiskSignalRepository extends JpaRepository<RiskSignalEntity, String> {
    List<RiskSignalEntity> findByPortfolioId(String portfolioId);
}
