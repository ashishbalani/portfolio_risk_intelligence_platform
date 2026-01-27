package com.portfolio.risk.api.repository;

import com.portfolio.risk.api.entity.PositionEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface PositionRepository extends JpaRepository<PositionEntity, String> {
    List<PositionEntity> findByPortfolioId(String portfolioId);
}
