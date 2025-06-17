package com.example.demo.config;

import com.example.demo.stream.SpanStreamProvider;

public class SpanResource {

    private KafkaConfig kafka;
    private FileConfig file;

    public KafkaConfig getKafka() { return kafka; }
    public void setKafka(KafkaConfig kafka) { this.kafka = kafka; }

    public FileConfig getFile() { return file; }
    public void setFile(FileConfig file) { this.file = file; }

    public SpanStreamProvider resolveProvider() {
        if (kafka != null) return kafka;
        if (file != null) return file;
        throw new IllegalStateException("No valid resource config found");
    }
}
