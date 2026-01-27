package com.portfolio.risk.common.dto;

import java.time.Instant;
import java.util.List;

public record PortfolioSnapshotDto(
        String portfolioId,
        Instant asOf,
        List<PositionDto> positions,
        List<ExposureDto> exposures,
        List<RiskSignalDto> signals
) {
}
