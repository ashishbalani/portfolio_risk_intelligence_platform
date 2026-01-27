package com.portfolio.risk.refdata.controller;

import com.portfolio.risk.refdata.entity.BookEntity;
import com.portfolio.risk.refdata.entity.PortfolioEntity;
import com.portfolio.risk.refdata.service.PortfolioService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/portfolios")
public class PortfolioController {
    private final PortfolioService portfolioService;

    public PortfolioController(PortfolioService portfolioService) {
        this.portfolioService = portfolioService;
    }

    @GetMapping
    public List<PortfolioEntity> portfolios() {
        return portfolioService.portfolios();
    }

    @GetMapping("/{portfolioId}/books")
    public List<BookEntity> books(@PathVariable String portfolioId) {
        return portfolioService.books(portfolioId);
    }
}
