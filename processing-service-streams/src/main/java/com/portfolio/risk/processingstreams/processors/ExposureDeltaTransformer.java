package com.portfolio.risk.processingstreams.processors;

import com.portfolio.risk.processingstreams.model.ExposureDelta;
import com.portfolio.risk.processingstreams.model.InstrumentRefData;
import com.portfolio.risk.processingstreams.model.Position;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class ExposureDeltaTransformer implements ValueTransformerWithKey<String, Position, Iterable<ExposureDelta>> {
    private final String prevStoreName;
    private final String refdataStoreName;

    private KeyValueStore<String, Position> prevStore;
    private ReadOnlyKeyValueStore<String, InstrumentRefData> refdataStore;

    public ExposureDeltaTransformer(String prevStoreName, String refdataStoreName) {
        this.prevStoreName = prevStoreName;
        this.refdataStoreName = refdataStoreName;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext context) {
        this.prevStore = (KeyValueStore<String, Position>) context.getStateStore(prevStoreName);
        this.refdataStore = (ReadOnlyKeyValueStore<String, InstrumentRefData>) context.getStateStore(refdataStoreName);
    }

    @Override
    public Iterable<ExposureDelta> transform(String key, Position value) {
        Position previous = prevStore.get(key);
        prevStore.put(key, value);

        BigDecimal prevGross = previous == null ? BigDecimal.ZERO : previous.grossNotionalBase();
        BigDecimal prevNet = previous == null ? BigDecimal.ZERO : previous.netNotionalBase();
        BigDecimal newGross = value.grossNotionalBase();
        BigDecimal newNet = value.netNotionalBase();

        BigDecimal grossDelta = newGross.subtract(prevGross);
        BigDecimal netDelta = newNet.subtract(prevNet);

        int prevCount = previous == null || previous.netQty().compareTo(BigDecimal.ZERO) == 0 ? 0 : 1;
        int newCount = value.netQty().compareTo(BigDecimal.ZERO) == 0 ? 0 : 1;
        int countDelta = newCount - prevCount;

        InstrumentRefData refdata = refdataStore.get(value.instrumentId());
        String assetClass = refdata == null || refdata.assetClass() == null ? "UNKNOWN" : refdata.assetClass();
        String sector = refdata == null || refdata.sector() == null ? "UNKNOWN" : refdata.sector();
        String country = refdata == null || refdata.country() == null ? "UNKNOWN" : refdata.country();

        List<ExposureDelta> deltas = new ArrayList<>();
        deltas.add(delta(value.portfolioId(), "instrument", value.instrumentId(), grossDelta, netDelta, countDelta));
        deltas.add(delta(value.portfolioId(), "assetClass", assetClass, grossDelta, netDelta, countDelta));
        deltas.add(delta(value.portfolioId(), "sector", sector, grossDelta, netDelta, countDelta));
        deltas.add(delta(value.portfolioId(), "country", country, grossDelta, netDelta, countDelta));
        deltas.add(delta(value.portfolioId(), "currency", value.instrumentCcy(), grossDelta, netDelta, countDelta));
        return deltas;
    }

    @Override
    public void close() {
    }

    private ExposureDelta delta(String portfolioId, String dimension, String bucket,
                                BigDecimal grossDelta, BigDecimal netDelta, int countDelta) {
        return new ExposureDelta(
                portfolioId,
                dimension,
                bucket == null ? "UNKNOWN" : bucket,
                grossDelta,
                netDelta,
                countDelta
        );
    }
}
