package com.portfolio.risk.api.service;

import com.portfolio.risk.api.entity.PositionEntity;
import com.portfolio.risk.api.entity.RiskSignalEntity;
import com.portfolio.risk.api.repository.PositionRepository;
import com.portfolio.risk.api.repository.RiskSignalRepository;
import com.portfolio.risk.common.dto.ExposureDto;
import com.portfolio.risk.common.dto.PortfolioSnapshotDto;
import com.portfolio.risk.common.dto.PositionDto;
import com.portfolio.risk.common.dto.RiskSignalDto;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class PortfolioQueryService {
    private final PositionRepository positionRepository;
    private final RiskSignalRepository riskSignalRepository;

    public PortfolioQueryService(PositionRepository positionRepository, RiskSignalRepository riskSignalRepository) {
        this.positionRepository = positionRepository;
        this.riskSignalRepository = riskSignalRepository;
    }

    public PortfolioSnapshotDto snapshot(String portfolioId) {
        List<PositionEntity> positions = positionRepository.findByPortfolioId(portfolioId);
        List<RiskSignalEntity> signals = riskSignalRepository.findByPortfolioId(portfolioId);

        List<PositionDto> positionDtos = positions.stream()
                .map(position -> new PositionDto(
                        position.getPortfolioId(),
                        position.getInstrumentId(),
                        position.getQuantity(),
                        position.getMarketValue(),
                        position.getCurrency()
                ))
                .toList();

        List<RiskSignalDto> signalDtos = signals.stream()
                .map(signal -> new RiskSignalDto(
                        signal.getSignalId(),
                        signal.getPortfolioId(),
                        signal.getType(),
                        signal.getValue(),
                        signal.getCreatedAt(),
                        signal.getSeverity()
                ))
                .toList();

        List<ExposureDto> exposures = aggregateExposures(portfolioId, positionDtos);

        return new PortfolioSnapshotDto(portfolioId, Instant.now(), positionDtos, exposures, signalDtos);
    }

    private List<ExposureDto> aggregateExposures(String portfolioId, List<PositionDto> positionDtos) {
        Map<String, BigDecimal> byCurrency = positionDtos.stream()
                .collect(Collectors.groupingBy(
                        PositionDto::currency,
                        Collectors.mapping(PositionDto::marketValue, Collectors.reducing(BigDecimal.ZERO, BigDecimal::add))
                ));

        return byCurrency.entrySet().stream()
                .map(entry -> new ExposureDto(portfolioId, entry.getKey(), entry.getValue()))
                .toList();
    }
}
