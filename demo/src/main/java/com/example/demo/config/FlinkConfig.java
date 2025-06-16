package com.example.demo.config;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FlinkConfig {
    @Bean
    public StreamExecutionEnvironment streamExecutionEnvironment() {
        return StreamExecutionEnvironment.createLocalEnvironment();
    }
}
