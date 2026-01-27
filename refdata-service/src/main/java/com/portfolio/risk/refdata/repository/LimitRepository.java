package com.portfolio.risk.refdata.repository;

import com.portfolio.risk.refdata.entity.LimitEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface LimitRepository extends JpaRepository<LimitEntity, String> {
    List<LimitEntity> findByPortfolioId(String portfolioId);
}
