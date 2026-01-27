package com.portfolio.risk.refdata.service;

import com.portfolio.risk.refdata.entity.BookEntity;
import com.portfolio.risk.refdata.entity.PortfolioEntity;
import com.portfolio.risk.refdata.repository.BookRepository;
import com.portfolio.risk.refdata.repository.PortfolioRepository;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class PortfolioService {
    private final PortfolioRepository portfolioRepository;
    private final BookRepository bookRepository;

    public PortfolioService(PortfolioRepository portfolioRepository, BookRepository bookRepository) {
        this.portfolioRepository = portfolioRepository;
        this.bookRepository = bookRepository;
    }

    public List<PortfolioEntity> portfolios() {
        return portfolioRepository.findAll();
    }

    public List<BookEntity> books(String portfolioId) {
        return bookRepository.findByPortfolioId(portfolioId);
    }
}
