package com.example.demo.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
@ConfigurationProperties(prefix = "")
public class ResourcesWrapper {
    private List<Map<String, Object>> resources;

    public List<Map<String, Object>> getResources() {
        return resources;
    }

    public void setResources(List<Map<String, Object>> resources) {
        this.resources = resources;
    }
}
