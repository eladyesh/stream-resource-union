package com.example.demo.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@ConfigurationProperties(prefix = "")
public class ResourcesWrapper {

    private List<SpanResource> resources;

    // No-arg constructor for Spring Boot to bind properties
    public ResourcesWrapper() {
    }

    // All-args constructor for unit tests
    public ResourcesWrapper(List<SpanResource> resources) {
        this.resources = resources;
    }

    public List<SpanResource> getResources() {
        return resources;
    }

    public void setResources(List<SpanResource> resources) {
        this.resources = resources;
    }
}
