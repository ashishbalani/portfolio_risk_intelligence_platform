package com.portfolio.risk.refdata.repository;

import com.portfolio.risk.refdata.entity.InstrumentEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface InstrumentRepository extends JpaRepository<InstrumentEntity, String> {
    List<InstrumentEntity> findBySymbolContainingIgnoreCaseOrNameContainingIgnoreCase(String symbol, String name);
}
