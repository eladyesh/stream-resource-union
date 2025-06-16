package com.example.demo.config;

import java.util.List;

public class KafkaResourcesWrapper {
    private List<KafkaResource> kafkaResources;

    public List<KafkaResource> getKafkaResources() {
        return kafkaResources;
    }

    public void setKafkaResources(List<KafkaResource> kafkaResources) {
        this.kafkaResources = kafkaResources;
    }
}
