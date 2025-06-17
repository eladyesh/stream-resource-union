package com.example.demo.config;

import java.util.List;

public class ResourcesWrapper {
    private List<SpanResource> resources;

    // No-arg constructor for YAML and serialization
    public ResourcesWrapper() {}

    // Convenience constructor for tests
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
