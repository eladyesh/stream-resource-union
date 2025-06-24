package com.example.demo.config;

import com.example.demo.stream.SpanStreamProvider;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class SpanResourceFactory {

    private final ObjectMapper objectMapper;

    private final Map<String, Class<? extends SpanStreamProvider>> resourceTypeToConfigClass = Map.of(
            "kafka", KafkaConfig.class,
            "file", FileConfig.class
    );

    public SpanResourceFactory(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public SpanResource createSpanResourceFromConfig(Map<String, Object> resourceTypeToConfigData) {
        if (resourceTypeToConfigData.size() != 1) {
            throw new IllegalArgumentException("Each resource must have exactly one type key");
        }

        String resourceType = resourceTypeToConfigData.keySet().iterator().next();
        Object configData = resourceTypeToConfigData.get(resourceType);

        Class<? extends SpanStreamProvider> resourceTypeToConfigClassValue = resourceTypeToConfigClass.get(resourceType);
        if (resourceTypeToConfigClassValue == null) {
            throw new IllegalArgumentException("Unknown resource type: " + resourceType);
        }

        SpanStreamProvider providerInstance = objectMapper.convertValue(configData, resourceTypeToConfigClassValue);
        return new SpanResource(providerInstance);
    }
}
