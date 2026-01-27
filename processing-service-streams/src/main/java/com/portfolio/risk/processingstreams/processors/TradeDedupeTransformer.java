package com.portfolio.risk.processingstreams.processors;

import com.portfolio.risk.common.dto.TradeEvent;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Instant;

public class TradeDedupeTransformer implements ValueTransformerWithKey<String, TradeEvent, TradeEvent> {
    private final String storeName;
    private KeyValueStore<String, Instant> store;

    public TradeDedupeTransformer(String storeName) {
        this.storeName = storeName;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext context) {
        this.store = (KeyValueStore<String, Instant>) context.getStateStore(storeName);
    }

    @Override
    public TradeEvent transform(String readOnlyKey, TradeEvent value) {
        if (value == null || value.eventId() == null) {
            return value;
        }
        String eventId = value.eventId().toString();
        Instant existing = store.get(eventId);
        if (existing != null) {
            return null;
        }
        store.put(eventId, value.eventTime());
        return value;
    }

    @Override
    public void close() {
    }
}
