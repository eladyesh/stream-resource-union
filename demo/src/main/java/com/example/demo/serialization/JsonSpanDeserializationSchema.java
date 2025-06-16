package com.example.demo.serialization;

import com.example.demo.model.Span;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class JsonSpanDeserializationSchema implements DeserializationSchema<Span> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Span deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, Span.class);
    }

    @Override
    public boolean isEndOfStream(Span nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Span> getProducedType() {
        return TypeInformation.of(Span.class);
    }
}
