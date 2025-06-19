package com.example.demo.config;

import com.example.demo.stream.SpanStreamProvider;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class SpanResourceFactory {

    private final ObjectMapper objectMapper;

    private final Map<String, Class<? extends SpanStreamProvider>> registry = Map.of(
            "kafka", KafkaConfig.class,
            "file", FileConfig.class
    );

    public SpanResourceFactory(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public SpanResource fromRaw(Map<String, Object> raw) {
        if (raw.size() != 1) throw new IllegalArgumentException("Each resource must have one type key");
        String type = raw.keySet().iterator().next();
        Object config = raw.get(type);

        Class<? extends SpanStreamProvider> clazz = registry.get(type);
        if (clazz == null) throw new IllegalArgumentException("Unknown resource type: " + type);

        SpanStreamProvider provider = objectMapper.convertValue(config, clazz);
        return new SpanResource(provider);
    }
}
