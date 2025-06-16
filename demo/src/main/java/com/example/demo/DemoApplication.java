package com.example.demo;

import com.example.demo.model.Span;
import com.example.demo.stream.KafkaSpanResource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class DemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }

    @Bean
    public StreamExecutionEnvironment streamEnv() {
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }

    @Bean
    public CommandLineRunner runner(KafkaSpanResource kafkaSpanResource, StreamExecutionEnvironment env) {
        return args -> {
            kafkaSpanResource.unifiedKafkaSpanStream(env)
                    .map(span -> ">> JSON SPAN: " + span)
                    .print();

            env.execute("Kafka Union Stream with JSON Deserialization");
        };
    }
}
