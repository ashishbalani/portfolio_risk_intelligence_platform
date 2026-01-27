package com.portfolio.risk.refdata.repository;

import com.portfolio.risk.refdata.entity.PortfolioEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PortfolioRepository extends JpaRepository<PortfolioEntity, String> {
}
