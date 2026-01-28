package com.portfolio.risk.processingstreams.processors;

import com.portfolio.risk.common.dto.TradeEvent;
import com.portfolio.risk.processingstreams.util.Keying;
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
        String eventIdKey = "eventId:" + value.eventId();
        Instant existing = store.get(eventIdKey);
        if (existing != null) {
            return null;
        }
        if (value.eventTime() != null) {
            String positionKey = "pos:" + Keying.positionKey(value.portfolioId(), value.bookId(), value.instrumentId());
            Instant lastEventTime = store.get(positionKey);
            if (lastEventTime != null && value.eventTime().isBefore(lastEventTime)) {
                return null;
            }
            store.put(positionKey, value.eventTime());
        }
        store.put(eventIdKey, value.eventTime());
        return value;
    }

    @Override
    public void close() {
    }
}
