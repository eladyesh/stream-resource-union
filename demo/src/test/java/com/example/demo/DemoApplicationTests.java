package com.example.demo;

import com.example.demo.model.Span;
import com.example.demo.stream.KafkaSpanResource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
public class DemoApplicationTests {

    @Test
    public void contextLoads(ApplicationContext context) {
        // Try to get beans from the context
        KafkaSpanResource kafkaSpanResource = context.getBean(KafkaSpanResource.class);

        // Simple checks that they are present
        assertThat(kafkaSpanResource).isNotNull();
    }

    private static final List<String> results = Collections.synchronizedList(new ArrayList<>());

    @Test
    public void testFlinkJob() throws Exception {
        results.clear();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Span> input = env.fromElements(
                new Span("trace1", "spanA", "serviceX"),
                new Span("trace2", "spanB", "serviceY")
        );

        DataStream<String> output = input.map(span ->
                span.getTraceId() + " - " + span.getService()
        );

        // Add a custom sink using anonymous class
        output.addSink(new SinkFunction<String>() {
            @Override
            public synchronized void invoke(String value, Context context) {
                results.add(value);
            }
        });

        env.execute("Test Flink Job");

        // Assertion
        assertThat(results).containsExactly(
                "trace1 - serviceX",
                "trace2 - serviceY"
        );
    }
}
