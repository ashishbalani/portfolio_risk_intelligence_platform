package com.portfolio.risk.processingstreams.serdes;

import com.portfolio.risk.common.dto.LimitConfigEvent;
import com.portfolio.risk.common.dto.FxRateEvent;
import com.portfolio.risk.common.dto.RefDataInstrumentEvent;
import com.portfolio.risk.common.dto.TradeEvent;
import com.portfolio.risk.processingstreams.model.ExposureBucket;
import com.portfolio.risk.processingstreams.model.ExposureDelta;
import com.portfolio.risk.processingstreams.model.ErrorEnvelope;
import com.portfolio.risk.processingstreams.model.FxRate;
import com.portfolio.risk.processingstreams.model.InstrumentRefData;
import com.portfolio.risk.processingstreams.model.LimitConfig;
import com.portfolio.risk.processingstreams.model.Position;
import com.portfolio.risk.processingstreams.model.RiskSignalEvent;
import org.apache.kafka.common.serialization.Serde;

public final class EventSerdes {
    private EventSerdes() {
    }

    public static Serde<TradeEvent> tradeEvent() {
        return new JsonSerde<>(TradeEvent.class);
    }

    public static Serde<RefDataInstrumentEvent> refDataInstrumentEvent() {
        return new JsonSerde<>(RefDataInstrumentEvent.class);
    }

    public static Serde<LimitConfigEvent> limitConfigEvent() {
        return new JsonSerde<>(LimitConfigEvent.class);
    }

    public static Serde<FxRateEvent> fxRateEvent() {
        return new JsonSerde<>(FxRateEvent.class);
    }

    public static Serde<InstrumentRefData> instrumentRefData() {
        return new JsonSerde<>(InstrumentRefData.class);
    }

    public static Serde<FxRate> fxRate() {
        return new JsonSerde<>(FxRate.class);
    }

    public static Serde<LimitConfig> limitConfig() {
        return new JsonSerde<>(LimitConfig.class);
    }

    public static Serde<Position> position() {
        return new JsonSerde<>(Position.class);
    }

    public static Serde<ExposureBucket> exposureBucket() {
        return new JsonSerde<>(ExposureBucket.class);
    }

    public static Serde<ExposureDelta> exposureDelta() {
        return new JsonSerde<>(ExposureDelta.class);
    }

    public static Serde<ErrorEnvelope> errorEnvelope() {
        return new JsonSerde<>(ErrorEnvelope.class);
    }

    public static Serde<RiskSignalEvent> riskSignalEvent() {
        return new JsonSerde<>(RiskSignalEvent.class);
    }
}
