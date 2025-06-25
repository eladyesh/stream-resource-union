package com.example.demo;

import com.example.demo.model.Span;
import com.example.demo.stream.SpanStreamBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

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
    @Profile("!test")
    public CommandLineRunner runner(SpanStreamBuilder builder, StreamExecutionEnvironment env) {
        return args -> {
            builder.buildUnifiedSpanStream(env)
                    .map(span -> ">> SPAN: " + span)
                    .print();

            env.execute("Unified Span Stream");
        };
    }
}
