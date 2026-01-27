package com.portfolio.risk.refdata.repository;

import com.portfolio.risk.refdata.entity.BookEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface BookRepository extends JpaRepository<BookEntity, String> {
    List<BookEntity> findByPortfolioId(String portfolioId);
}
