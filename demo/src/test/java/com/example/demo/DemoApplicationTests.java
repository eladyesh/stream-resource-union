package com.example.demo;

import com.example.demo.config.ResourceConfigLoader;
import com.example.demo.config.ResourcesWrapper;
import com.example.demo.config.SpanResource;
import com.example.demo.model.Span;
import com.example.demo.stream.SpanStreamBuilder;
import com.example.demo.stream.SpanStreamProvider;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@SpringBootTest
@ActiveProfiles("test")  // Use the 'test' profile to isolate config and beans for testing
public class DemoApplicationTests {

    // Thread-safe list to collect results emitted from Flink sinks during test runs
    private static final List<String> collected = Collections.synchronizedList(new ArrayList<>());

    // This method runs before each test method to clear the collected results list
    @BeforeEach
    void beforeEach() {
        collected.clear();
    }

    /**
     * Basic Spring context load test:
     * Ensures that Spring context boots up properly and required beans are available.
     * - Checks SpanStreamBuilder bean is loaded (our core stream builder component)
     * - Checks ResourceConfigLoader bean is loaded (our YAML config loader)
     */
    @Test
    public void contextLoads(ApplicationContext context) {
        assertThat(context.getBean(SpanStreamBuilder.class)).isNotNull();
        assertThat(context.getBean(ResourceConfigLoader.class)).isNotNull();
    }

    /**
     * End-to-end test of SpanStreamBuilder using mocks:
     * This test verifies the core stream building logic without connecting to real external systems.
     * It mocks the config loader and stream providers to supply controlled test data.
     */
    @Test
    public void testUnifiedStreamWithMocks() throws Exception {
        // Create a mock ResourceConfigLoader to avoid reading real config files
        ResourceConfigLoader mockLoader = mock(ResourceConfigLoader.class);

        // Instantiate SpanStreamBuilder with the mocked loader
        SpanStreamBuilder builder = new SpanStreamBuilder(mockLoader);

        // Create two mocked SpanResource objects representing two separate sources
        SpanResource resource1 = mock(SpanResource.class);
        SpanResource resource2 = mock(SpanResource.class);

        // Create mocked SpanStreamProvider objects, which build streams from resources
        SpanStreamProvider provider1 = mock(SpanStreamProvider.class);
        SpanStreamProvider provider2 = mock(SpanStreamProvider.class);

        // Set up Flink streaming environment for the test with parallelism 1 for determinism
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Create two bounded DataStreams using fromElements(), each emitting one Span instance
        DataStream<Span> stream1 = env.fromElements(new Span("trace1", "spanA", "serviceX"));
        DataStream<Span> stream2 = env.fromElements(new Span("trace2", "spanB", "serviceY"));

        // Configure the mocked resources to return their respective providers when resolved
        when(resource1.resolveProvider()).thenReturn(provider1);
        when(resource2.resolveProvider()).thenReturn(provider2);

        // Configure the mocked providers to return the predefined streams when buildStream is called
        when(provider1.buildStream(env)).thenReturn(stream1);
        when(provider2.buildStream(env)).thenReturn(stream2);

        // Configure the mock loader to return a ResourcesWrapper containing our two mocked resources
        when(mockLoader.loadConfig(anyString())).thenReturn(
                new ResourcesWrapper(Arrays.asList(resource1, resource2))
        );

        // Call the method under test: build the unified stream from all resources
        DataStream<Span> unifiedStream = builder.unifiedStream(env);

        // Map each Span into a simple concatenated string to ease verification
        DataStream<String> output = unifiedStream.map(span ->
                span.getTraceId() + "|" + span.getSpan() + "|" + span.getService()
        );

        // Add a custom SinkFunction that collects emitted output strings into the synchronized list
        output.addSink(new SinkFunction<String>() {
            @Override
            public synchronized void invoke(String value, Context context) {
                collected.add(value);
            }
        });

        // Trigger Flink job execution; this runs the streams and collects results in the sink
        env.execute("Mocked SpanStreamBuilder Test");

        // Assert that all expected concatenated Span strings were collected, order agnostic
        assertThat(collected).containsExactlyInAnyOrder(
                "trace1|spanA|serviceX",
                "trace2|spanB|serviceY"
        );

        // Verify that the mock methods were called exactly once as expected, ensuring proper flow
        verify(mockLoader, times(1)).loadConfig(anyString());
        verify(resource1, times(1)).resolveProvider();
        verify(resource2, times(1)).resolveProvider();
        verify(provider1, times(1)).buildStream(env);
        verify(provider2, times(1)).buildStream(env);
    }
}
