package com.portfolio.risk.refdata.controller;

import com.portfolio.risk.refdata.entity.LimitEntity;
import com.portfolio.risk.refdata.service.LimitService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.List;

@RestController
@RequestMapping("/limits")
public class LimitController {
    private final LimitService limitService;

    public LimitController(LimitService limitService) {
        this.limitService = limitService;
    }

    @GetMapping
    public List<LimitEntity> limits(@RequestParam(value = "portfolioId", required = false) String portfolioId) {
        return limitService.findByPortfolio(portfolioId);
    }

    @PostMapping
    @ResponseStatus(HttpStatus.OK)
    public LimitEntity upsert(@Valid @RequestBody LimitUpsertRequest request) {
        LimitEntity entity = new LimitEntity();
        entity.setLimitId(request.limitId());
        entity.setPortfolioId(request.portfolioId());
        entity.setBookId(request.bookId());
        entity.setLimitType(request.limitType());
        entity.setThreshold(request.threshold());
        entity.setCurrency(request.currency());
        return limitService.upsert(entity);
    }

    public record LimitUpsertRequest(
            @NotBlank String limitId,
            @NotBlank String portfolioId,
            @NotBlank String bookId,
            @NotBlank String limitType,
            @NotNull @PositiveOrZero BigDecimal threshold,
            @NotBlank String currency
    ) {
    }
}
