package com.portfolio.risk.api.controller;

import com.portfolio.risk.api.repository.PositionRepository;
import com.portfolio.risk.api.repository.RiskSignalRepository;
import com.portfolio.risk.api.service.PortfolioQueryService;
import com.portfolio.risk.common.dto.PortfolioSnapshotDto;
import com.portfolio.risk.common.dto.PositionDto;
import com.portfolio.risk.common.dto.RiskSignalDto;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1/portfolios")
public class PortfolioController {
    private final PortfolioQueryService portfolioQueryService;
    private final PositionRepository positionRepository;
    private final RiskSignalRepository riskSignalRepository;

    public PortfolioController(PortfolioQueryService portfolioQueryService,
                               PositionRepository positionRepository,
                               RiskSignalRepository riskSignalRepository) {
        this.portfolioQueryService = portfolioQueryService;
        this.positionRepository = positionRepository;
        this.riskSignalRepository = riskSignalRepository;
    }

    @GetMapping("/{portfolioId}/snapshot")
    public PortfolioSnapshotDto snapshot(@PathVariable String portfolioId) {
        return portfolioQueryService.snapshot(portfolioId);
    }

    @GetMapping("/{portfolioId}/positions")
    public List<PositionDto> positions(@PathVariable String portfolioId) {
        return positionRepository.findByPortfolioId(portfolioId).stream()
                .map(position -> new PositionDto(
                        position.getPortfolioId(),
                        position.getInstrumentId(),
                        position.getQuantity(),
                        position.getMarketValue(),
                        position.getCurrency()
                ))
                .toList();
    }

    @GetMapping("/{portfolioId}/signals")
    public List<RiskSignalDto> signals(@PathVariable String portfolioId) {
        return riskSignalRepository.findByPortfolioId(portfolioId).stream()
                .map(signal -> new RiskSignalDto(
                        signal.getSignalId(),
                        signal.getPortfolioId(),
                        signal.getType(),
                        signal.getValue(),
                        signal.getCreatedAt(),
                        signal.getSeverity()
                ))
                .toList();
    }
}
