package com.portfolio.risk.processingstreams.topology;

import com.portfolio.risk.common.dto.TradeEvent;
import com.portfolio.risk.processingstreams.config.StreamsProps;
import com.portfolio.risk.processingstreams.metrics.StreamMetrics;
import com.portfolio.risk.processingstreams.model.*;
import com.portfolio.risk.processingstreams.processors.ExposureDeltaTransformer;
import com.portfolio.risk.processingstreams.processors.RiskSignalTransformer;
import com.portfolio.risk.processingstreams.processors.TradeDedupeTransformer;
import com.portfolio.risk.processingstreams.serdes.EventSerdes;
import com.portfolio.risk.processingstreams.serdes.JsonSerde;
import com.portfolio.risk.processingstreams.stores.StateStores;
import com.portfolio.risk.processingstreams.util.Keying;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;

@Configuration
public class StreamsTopology {
    private final StreamsProps streamsProps;
    private final StreamMetrics metrics;
    private final com.fasterxml.jackson.databind.ObjectMapper mapper = JsonSerde.defaultMapper();

    public StreamsTopology(StreamsProps streamsProps, StreamMetrics metrics) {
        this.streamsProps = streamsProps;
        this.metrics = metrics;
    }

    @Bean
    public Topology topology(StreamsBuilder builder) {
        JsonSerde<TradeEvent> tradeSerde = new JsonSerde<>(TradeEvent.class);
        JsonSerde<ErrorEnvelope> dlqSerde = new JsonSerde<>(ErrorEnvelope.class);
        JsonSerde<InstrumentRefData> refDataSerde = new JsonSerde<>(InstrumentRefData.class);
        JsonSerde<FxRate> fxSerde = new JsonSerde<>(FxRate.class);
        JsonSerde<LimitConfig> limitSerde = new JsonSerde<>(LimitConfig.class);
        JsonSerde<Position> positionSerde = new JsonSerde<>(Position.class);
        JsonSerde<ExposureBucket> exposureSerde = new JsonSerde<>(ExposureBucket.class);
        JsonSerde<RiskSignalEvent> riskSerde = new JsonSerde<>(RiskSignalEvent.class);

        KStream<String, TradeEvent> trades = builder.stream(
                streamsProps.getTopics().getTrades(),
                Consumed.with(Serdes.String(), tradeSerde)
        );

        KTable<String, InstrumentRefData> refdataTable = builder.table(
                streamsProps.getTopics().getRefdata(),
                Consumed.with(Serdes.String(), refDataSerde),
                Materialized.<String, InstrumentRefData, KeyValueStore<Bytes, byte[]>>as("refdata-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(refDataSerde)
                        .withLoggingEnabled(storeLoggingConfig())
        );

        KTable<String, FxRate> fxTable = builder.table(
                streamsProps.getTopics().getFx(),
                Consumed.with(Serdes.String(), fxSerde),
                Materialized.<String, FxRate, KeyValueStore<Bytes, byte[]>>as("fx-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(fxSerde)
                        .withLoggingEnabled(storeLoggingConfig())
        );

        KTable<String, LimitConfig> limitTable = builder.table(
                streamsProps.getTopics().getLimits(),
                Consumed.with(Serdes.String(), limitSerde),
                Materialized.<String, LimitConfig, KeyValueStore<Bytes, byte[]>>as("limits-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(limitSerde)
                        .withLoggingEnabled(storeLoggingConfig())
        );

        KStream<String, TradeEvent>[] branches = trades.branch(
                (key, value) -> isValidTrade(value),
                (key, value) -> true
        );

        KStream<String, TradeEvent> validTrades = branches[0]
                .peek((key, value) -> recordStage("trades.validated", value));

        KStream<String, ErrorEnvelope> invalidTrades = branches[1]
                .mapValues(value -> new ErrorEnvelope(
                        streamsProps.getTopics().getTrades(),
                        value == null ? null : value.tradeId(),
                        toJsonSafe(value),
                        "VALIDATION_ERROR",
                        "Trade validation failed",
                        null,
                        Instant.now(),
                        value == null || value.eventId() == null ? null : value.eventId().toString()
                ));

        invalidTrades.to(streamsProps.getTopics().getTradesDlq(), Produced.with(Serdes.String(), dlqSerde));

        KeyValueBytesStoreSupplier dedupeStore = Stores.persistentKeyValueStore("trades-dedupe-store");
        builder.addStateStore(Stores.keyValueStoreBuilder(dedupeStore, Serdes.String(), new JsonSerde<>(Instant.class))
                .withLoggingEnabled(storeLoggingConfig()));

        KStream<String, TradeEvent> dedupedTrades = validTrades
                .transformValues(() -> new TradeDedupeTransformer("trades-dedupe-store"), "trades-dedupe-store")
                .filter((key, value) -> value != null)
                .peek((key, value) -> recordStage("trades.deduped", value.eventTime()));

        KStream<String, TradeEvent> tradesByInstrument = dedupedTrades
                .selectKey((key, value) -> value.instrumentId())
                .through("trades-by-instrument", Produced.with(Serdes.String(), tradeSerde));

        KStream<String, EnrichedTrade> enriched = tradesByInstrument
                .leftJoin(refdataTable, (trade, ref) -> EnrichedTrade.fromTrade(trade, ref))
                .selectKey((key, value) -> fxKey(value.trade().currency()))
                .through("trades-by-fx", Produced.with(Serdes.String(), new JsonSerde<>(EnrichedTrade.class)))
                .leftJoin(fxTable, (trade, fx) -> trade.withFx(fx, isFxMissing(trade.trade().currency(), fx)))
                .peek((key, value) -> recordStage("trades.enriched", value.trade().eventTime()));

        KTable<String, Position> positions = enriched
                .selectKey((key, value) -> Keying.positionKey(
                        value.trade().portfolioId(),
                        value.trade().bookId(),
                        value.trade().instrumentId()
                ))
                .through("trades-by-position", Produced.with(Serdes.String(), new JsonSerde<>(EnrichedTrade.class)))
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(EnrichedTrade.class)))
                .aggregate(
                        () -> new Position("", "", "", "", streamsProps.getBaseCurrency(), BigDecimal.ZERO, BigDecimal.ZERO,
                                BigDecimal.ZERO, BigDecimal.ZERO, null, null),
                        (key, value, aggregate) -> aggregatePosition(value, aggregate),
                        Materialized.<String, Position, KeyValueStore<Bytes, byte[]>>as(StateStores.POSITIONS_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(positionSerde)
                                .withCachingEnabled()
                                .withLoggingEnabled(storeLoggingConfig())
                );

        KStream<String, Position> positionUpdates = positions.toStream();
        if (streamsProps.isSuppressOutputs()) {
            positionUpdates = positions.suppress(Suppressed.untilTimeLimit(java.time.Duration.ofSeconds(1), Suppressed.BufferConfig.unbounded()))
                    .toStream();
        }
        positionUpdates
                .peek((key, value) -> recordStage("positions.current", value.lastEventTime()))
                .to(streamsProps.getTopics().getPositionsCurrent(), Produced.with(Serdes.String(), positionSerde));

        KeyValueBytesStoreSupplier previousPositionsStore = Stores.inMemoryKeyValueStore("positions-prev-store");
        builder.addStateStore(Stores.keyValueStoreBuilder(previousPositionsStore, Serdes.String(), positionSerde));

        Serde<ExposureDelta> exposureDeltaSerde = EventSerdes.exposureDelta();
        KStream<String, ExposureDelta> exposureDeltas = positions.toStream()
                .flatTransformValues(
                        () -> new ExposureDeltaTransformer("positions-prev-store", "refdata-store"),
                        "positions-prev-store",
                        "refdata-store"
                );

        KTable<String, ExposureBucket> exposures = exposureDeltas
                .selectKey((key, value) -> Keying.exposureKey(value.portfolioId(), value.dimension(), value.bucket()))
                .groupByKey(Grouped.with(Serdes.String(), exposureDeltaSerde))
                .aggregate(
                        () -> new ExposureBucket("", "", "", BigDecimal.ZERO, BigDecimal.ZERO, 0, null),
                        (key, delta, aggregate) -> new ExposureBucket(
                                delta.portfolioId(),
                                delta.dimension(),
                                delta.bucket(),
                                aggregate.grossNotionalBase().add(delta.grossNotionalDelta()),
                                aggregate.netNotionalBase().add(delta.netNotionalDelta()),
                                aggregate.positionCount() + delta.positionDelta(),
                                Instant.now()
                        ),
                        Materialized.<String, ExposureBucket, KeyValueStore<Bytes, byte[]>>as("exposures-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(exposureSerde)
                                .withCachingEnabled()
                                .withLoggingEnabled(storeLoggingConfig())
                );

        KStream<String, ExposureBucket> exposureUpdates = exposures.toStream();
        if (streamsProps.isSuppressOutputs()) {
            exposureUpdates = exposures.suppress(Suppressed.untilTimeLimit(java.time.Duration.ofSeconds(1), Suppressed.BufferConfig.unbounded()))
                    .toStream();
        }
        exposureUpdates
                .peek((key, value) -> recordStage("exposures.current", value.updatedAt()))
                .to(streamsProps.getTopics().getExposuresCurrent(), Produced.with(Serdes.String(), exposureSerde));

        KeyValueBytesStoreSupplier signalsStore = Stores.persistentKeyValueStore("signals-open-store");
        builder.addStateStore(Stores.keyValueStoreBuilder(signalsStore, Serdes.String(), new JsonSerde<>(OpenSignalState.class))
                .withLoggingEnabled(storeLoggingConfig()));

        KStream<String, RiskSignalEvent> riskSignals = exposures.toStream()
                .selectKey((key, value) -> value.portfolioId())
                .through("exposures-by-portfolio", Produced.with(Serdes.String(), exposureSerde))
                .leftJoin(limitTable, (exposure, limit) -> new ExposureWithLimit(exposure, limit))
                .flatTransformValues(() -> new RiskSignalTransformer("signals-open-store", true), "signals-open-store")
                .peek((key, value) -> recordStage("risk.signals", value.createdAt()));

        riskSignals.to(streamsProps.getTopics().getRiskSignals(), Produced.with(Serdes.String(), riskSerde));
        riskSignals
                .filter((key, value) -> "OPEN".equalsIgnoreCase(value.status()))
                .selectKey((key, value) -> Keying.exposureKey(value.portfolioId(), value.type(), value.bucket()))
                .to(streamsProps.getTopics().getRiskSignalsCurrent(), Produced.with(Serdes.String(), riskSerde));

        return builder.build();
    }

    private boolean isValidTrade(TradeEvent trade) {
        if (trade == null) {
            return false;
        }
        if (trade.eventId() == null || trade.eventTime() == null) {
            return false;
        }
        if (isBlank(trade.tradeId()) || isBlank(trade.portfolioId()) || isBlank(trade.bookId())) {
            return false;
        }
        if (isBlank(trade.instrumentId()) || trade.quantity() == null || trade.price() == null) {
            return false;
        }
        return trade.quantity().compareTo(BigDecimal.ZERO) > 0 && trade.price().compareTo(BigDecimal.ZERO) > 0;
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private void recordStage(String stage, TradeEvent trade) {
        metrics.increment(stage);
        metrics.recordLatency(stage, trade.eventTime());
    }

    private void recordStage(String stage, Instant eventTime) {
        metrics.increment(stage);
        metrics.recordLatency(stage, eventTime);
    }

    private String fxKey(String currency) {
        String baseCcy = streamsProps.getBaseCurrency();
        if (currency == null || currency.isBlank() || currency.equalsIgnoreCase(baseCcy)) {
            return baseCcy.toUpperCase() + "/" + baseCcy.toUpperCase();
        }
        return currency.toUpperCase() + "/" + baseCcy.toUpperCase();
    }

    private Position aggregatePosition(EnrichedTrade trade, Position aggregate) {
        if (aggregate.lastEventTime() != null && trade.trade().eventTime().isBefore(aggregate.lastEventTime())) {
            return aggregate;
        }
        BigDecimal signedQty = "SELL".equalsIgnoreCase(trade.trade().side())
                ? trade.trade().quantity().negate()
                : trade.trade().quantity();

        BigDecimal prevQty = aggregate.netQty();
        BigDecimal newQty = prevQty.add(signedQty);

        BigDecimal prevAbs = prevQty.abs();
        BigDecimal signedAbs = signedQty.abs();
        BigDecimal newAbs = newQty.abs();

        // Avg price rules (deterministic):
        // 1) Increasing same-side position -> weighted avg.
        // 2) Reducing position -> keep previous avg.
        // 3) Flat -> avg resets to 0.
        // 4) Flip -> avg becomes trade price for remaining position.
        BigDecimal avgPrice = aggregate.avgPrice();
        if (prevQty.compareTo(BigDecimal.ZERO) == 0) {
            avgPrice = trade.trade().price();
        } else if (prevQty.signum() == signedQty.signum()) {
            BigDecimal weighted = avgPrice.multiply(prevAbs).add(trade.trade().price().multiply(signedAbs));
            avgPrice = safeDivide(weighted, newAbs);
        } else if (signedAbs.compareTo(prevAbs) < 0) {
            // reduction: keep previous avg price for remaining position
        } else if (signedAbs.compareTo(prevAbs) == 0) {
            // flat: reset avg price to zero deterministically
            avgPrice = BigDecimal.ZERO;
        } else {
            // flip: remaining position inherits trade price
            avgPrice = trade.trade().price();
        }

        String baseCcy = streamsProps.getBaseCurrency();
        String instrumentCcy = trade.refData() == null || trade.refData().currency() == null
                ? trade.trade().currency()
                : trade.refData().currency();
        BigDecimal fxRate = resolveFxRate(instrumentCcy, baseCcy, trade.fxRate(), trade.fxMissing());

        BigDecimal grossNotional = avgPrice.multiply(newAbs).multiply(fxRate);
        BigDecimal netNotional = avgPrice.multiply(newQty).multiply(fxRate);

        return new Position(
                trade.trade().portfolioId(),
                trade.trade().bookId(),
                trade.trade().instrumentId(),
                instrumentCcy,
                baseCcy,
                newQty,
                avgPrice,
                grossNotional,
                netNotional,
                trade.trade().eventTime(),
                Instant.now()
        );
    }


    private BigDecimal resolveFxRate(String instrumentCcy, String baseCcy, FxRate fxRate, boolean fxMissing) {
        if (instrumentCcy == null || baseCcy == null) {
            return BigDecimal.ZERO;
        }
        if (instrumentCcy.equalsIgnoreCase(baseCcy)) {
            return BigDecimal.ONE;
        }
        if (fxMissing || fxRate == null || fxRate.rate() == null) {
            return BigDecimal.ZERO;
        }
        return fxRate.rate();
    }

    private boolean isFxMissing(String currency, FxRate fxRate) {
        if (currency == null || currency.equalsIgnoreCase(streamsProps.getBaseCurrency())) {
            return false;
        }
        return fxRate == null || fxRate.rate() == null;
    }

    private BigDecimal safeDivide(BigDecimal numerator, BigDecimal denominator) {
        if (denominator == null || denominator.compareTo(BigDecimal.ZERO) == 0) {
            return BigDecimal.ZERO;
        }
        return numerator.divide(denominator, 8, RoundingMode.HALF_UP);
    }

    private String toJsonSafe(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (Exception ex) {
            return null;
        }
    }

    private java.util.Map<String, String> storeLoggingConfig() {
        java.util.Map<String, String> config = new java.util.HashMap<>();
        config.put("cleanup.policy", "compact");
        return config;
    }

}
