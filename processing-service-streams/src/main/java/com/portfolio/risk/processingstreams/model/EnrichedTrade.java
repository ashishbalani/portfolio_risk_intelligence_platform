package com.portfolio.risk.processingstreams.model;

import com.portfolio.risk.common.dto.TradeEvent;

public record EnrichedTrade(
        TradeEvent trade,
        InstrumentRefData refData,
        FxRate fxRate,
        boolean refDataMissing,
        boolean fxMissing
) {
    public static EnrichedTrade fromTrade(TradeEvent trade, InstrumentRefData refData) {
        return new EnrichedTrade(trade, refData, null, refData == null, false);
    }

    public EnrichedTrade withFx(FxRate fxRate, boolean fxMissing) {
        return new EnrichedTrade(trade, refData, fxRate, refDataMissing, fxMissing);
    }
}
