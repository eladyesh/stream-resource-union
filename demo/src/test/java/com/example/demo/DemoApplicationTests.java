package com.example.demo;

import com.example.demo.config.ResolvedSpanResources;
import com.example.demo.config.ResourcesWrapper;
import com.example.demo.config.SpanResource;
import com.example.demo.config.SpanResourceFactory;
import com.example.demo.model.Span;
import com.example.demo.stream.SpanStreamBuilder;
import com.example.demo.stream.SpanStreamProvider;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@SpringBootTest
@ActiveProfiles("test")
public class DemoApplicationTests {

    private static final List<String> collected = Collections.synchronizedList(new ArrayList<>());

    @MockBean
    private ResourcesWrapper resourcesWrapper;

    @MockBean
    private SpanResourceFactory spanResourceFactory;

    @BeforeEach
    void beforeEach() {
        collected.clear();
    }

    @Test
    public void contextLoads(ApplicationContext context) {
        assertThat(context.getBean(SpanStreamBuilder.class)).isNotNull();
        assertThat(context.getBean(ResourcesWrapper.class)).isNotNull();
    }

    @Test
    public void testUnifiedStreamWithMocks() throws Exception {
        // Create mocks for providers
        SpanStreamProvider provider1 = mock(SpanStreamProvider.class);
        SpanStreamProvider provider2 = mock(SpanStreamProvider.class);

        // Create SpanResources wrapping those providers
        SpanResource resource1 = new SpanResource(provider1);
        SpanResource resource2 = new SpanResource(provider2);

        // Stub ResourcesWrapper to return raw configs (simulate your raw YAML map objects)
        // Here, we mock raw config maps for two resources
        List<Map<String, Object>> rawResources = List.of(
                Map.of("kafka", Map.of("topic", "otel-traces-a")),
                Map.of("file", Map.of("path", "spans.json"))
        );
        resourcesWrapper.setResources(rawResources);

        // Mock factory to convert raw maps to SpanResource (return our prepared SpanResources)
        when(resourcesWrapper.getResources()).thenReturn(rawResources);
        when(spanResourceFactory.fromRaw(any())).thenReturn(resource1, resource2);

        // Set up Flink environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Prepare example DataStreams
        DataStream<Span> stream1 = env.fromElements(new Span("trace1", "spanA", "serviceX"));
        DataStream<Span> stream2 = env.fromElements(new Span("trace2", "spanB", "serviceY"));

        when(provider1.buildStream(env)).thenReturn(stream1);
        when(provider2.buildStream(env)).thenReturn(stream2);

        // Use constructor that takes raw resources and factory to resolve SpanResources
        ResolvedSpanResources resolved = new ResolvedSpanResources(resourcesWrapper, spanResourceFactory);

        SpanStreamBuilder builder = new SpanStreamBuilder(resolved);

        DataStream<Span> unifiedStream = builder.buildUnifiedSpanStream(env);

        DataStream<String> output = unifiedStream.map(span ->
                span.getTraceId() + "|" + span.getSpan() + "|" + span.getService()
        );

        output.addSink(new SinkFunction<>() {
            @Override
            public synchronized void invoke(String value, Context context) {
                collected.add(value);
            }
        });

        env.execute("Mocked SpanStreamBuilder Test");

        assertThat(collected).containsExactlyInAnyOrder(
                "trace1|spanA|serviceX",
                "trace2|spanB|serviceY"
        );

        // Verify mocks
        verify(spanResourceFactory, times(2)).fromRaw(any());
        verify(provider1).buildStream(env);
        verify(provider2).buildStream(env);
    }
}
