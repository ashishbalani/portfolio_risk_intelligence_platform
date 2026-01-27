package com.portfolio.risk.processingstreams.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class JsonSerde<T> implements Serde<T> {
    private final Serde<T> delegate;

    public JsonSerde(Class<T> targetType) {
        this(targetType, defaultMapper());
    }

    public JsonSerde(Class<T> targetType, ObjectMapper mapper) {
        JsonSerializer<T> serializer = new JsonSerializer<>(mapper);
        JsonDeserializer<T> deserializer = new JsonDeserializer<>(targetType, mapper, false);
        deserializer.addTrustedPackages("*");
        this.delegate = Serdes.serdeFrom(serializer, deserializer);
    }

    @Override
    public Serializer<T> serializer() {
        return delegate.serializer();
    }

    @Override
    public Deserializer<T> deserializer() {
        return delegate.deserializer();
    }

    public static ObjectMapper defaultMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }
}
