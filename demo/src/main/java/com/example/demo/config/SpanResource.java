package com.example.demo.config;

import com.example.demo.stream.SpanStreamProvider;

public class SpanResource {
    private final SpanStreamProvider provider;

    public SpanResource(SpanStreamProvider provider) {
        this.provider = provider;
    }

    public SpanStreamProvider resolveProvider() {
        return provider;
    }
}
