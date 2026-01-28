package com.portfolio.risk.processingstreams.processors;

import com.portfolio.risk.processingstreams.model.*;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class RiskSignalTransformer implements ValueTransformerWithKey<String, ExposureWithLimit, Iterable<RiskSignalEvent>> {
    private final String stateStoreName;
    private final boolean emitClose;

    private KeyValueStore<String, OpenSignalState> stateStore;

    public RiskSignalTransformer(String stateStoreName, boolean emitClose) {
        this.stateStoreName = stateStoreName;
        this.emitClose = emitClose;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext context) {
        this.stateStore = (KeyValueStore<String, OpenSignalState>) context.getStateStore(stateStoreName);
    }

    @Override
    public Iterable<RiskSignalEvent> transform(String key, ExposureWithLimit value) {
        ExposureBucket exposure = value.exposure();
        LimitConfig limit = value.limit();
        if (exposure == null || limit == null) {
            return List.of();
        }

        List<RiskSignalEvent> events = new ArrayList<>();
        BigDecimal nav = limit.nav();
        BigDecimal gross = exposure.grossNotionalBase();

        // absolute cap
        if (limit.grossNotionalCap() != null) {
            evaluate("GROSS_NOTIONAL_CAP", exposure, gross, limit.grossNotionalCap(), true, events);
        }

        // pct checks if NAV available
        if (nav != null && nav.compareTo(BigDecimal.ZERO) > 0) {
            BigDecimal pct = safeDivide(gross, nav);
            String dimension = exposure.dimension();
            if ("instrument".equalsIgnoreCase(dimension) && limit.maxSingleNamePct() != null) {
                evaluate("MAX_SINGLE_NAME_PCT", exposure, pct, limit.maxSingleNamePct(), false, events);
            }
            if ("sector".equalsIgnoreCase(dimension) && limit.maxSectorPct() != null) {
                evaluate("MAX_SECTOR_PCT", exposure, pct, limit.maxSectorPct(), false, events);
            }
            if ("currency".equalsIgnoreCase(dimension) && limit.maxFxPct() != null) {
                evaluate("MAX_FX_PCT", exposure, pct, limit.maxFxPct(), false, events);
            }
        }

        return events;
    }

    @Override
    public void close() {
    }

    private void evaluate(String signalType,
                          ExposureBucket exposure,
                          BigDecimal value,
                          BigDecimal threshold,
                          boolean absolute,
                          List<RiskSignalEvent> events) {
        boolean breached = value != null && threshold != null && value.compareTo(threshold) > 0;
        String stateKey = stateKey(exposure.portfolioId(), signalType, exposure.bucket());
        OpenSignalState state = stateStore.get(stateKey);
        boolean open = state != null && state.open();

        if (breached && !open) {
            stateStore.put(stateKey, new OpenSignalState(true, Instant.now()));
            events.add(openSignal(signalType, exposure, value, threshold));
            return;
        }

        if (!breached && open && emitClose) {
            stateStore.put(stateKey, new OpenSignalState(false, Instant.now()));
            events.add(closeSignal(signalType, exposure, value, threshold));
        }
    }

    private RiskSignalEvent openSignal(String type, ExposureBucket exposure, BigDecimal value, BigDecimal threshold) {
        return new RiskSignalEvent(
                "SIG-" + UUID.randomUUID(),
                exposure.portfolioId(),
                type,
                exposure.dimension(),
                exposure.bucket(),
                value,
                threshold,
                "OPEN",
                "HIGH",
                Instant.now()
        );
    }

    private RiskSignalEvent closeSignal(String type, ExposureBucket exposure, BigDecimal value, BigDecimal threshold) {
        return new RiskSignalEvent(
                "SIG-" + UUID.randomUUID(),
                exposure.portfolioId(),
                type,
                exposure.dimension(),
                exposure.bucket(),
                value,
                threshold,
                "CLOSED",
                "NORMAL",
                Instant.now()
        );
    }

    private String stateKey(String portfolioId, String type, String bucket) {
        return (portfolioId == null ? "" : portfolioId) + "|" + type + "|" + (bucket == null ? "" : bucket);
    }

    private BigDecimal safeDivide(BigDecimal numerator, BigDecimal denominator) {
        if (denominator == null || denominator.compareTo(BigDecimal.ZERO) == 0) {
            return BigDecimal.ZERO;
        }
        return numerator.divide(denominator, 8, java.math.RoundingMode.HALF_UP);
    }
}
