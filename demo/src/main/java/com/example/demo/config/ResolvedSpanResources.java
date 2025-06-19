package com.example.demo.config;

import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class ResolvedSpanResources {

    private final List<SpanResource> resources;

    public ResolvedSpanResources(ResourcesWrapper wrapper, SpanResourceFactory factory) {
        this.resources = wrapper.getResources()
                .stream()
                .map(factory::fromRaw)
                .collect(Collectors.toList());
    }

    public List<SpanResource> getResources() {
        return resources;
    }
}
