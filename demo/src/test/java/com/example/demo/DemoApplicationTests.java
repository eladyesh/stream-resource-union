package com.example.demo;

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
@ActiveProfiles("test")
public class DemoApplicationTests {

    private static final List<String> collected = Collections.synchronizedList(new ArrayList<>());

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
        // Mock SpanResource and SpanStreamProvider as before
        SpanResource resource1 = mock(SpanResource.class);
        SpanResource resource2 = mock(SpanResource.class);

        SpanStreamProvider provider1 = mock(SpanStreamProvider.class);
        SpanStreamProvider provider2 = mock(SpanStreamProvider.class);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Span> stream1 = env.fromElements(new Span("trace1", "spanA", "serviceX"));
        DataStream<Span> stream2 = env.fromElements(new Span("trace2", "spanB", "serviceY"));

        when(resource1.resolveProvider()).thenReturn(provider1);
        when(resource2.resolveProvider()).thenReturn(provider2);

        when(provider1.buildStream(env)).thenReturn(stream1);
        when(provider2.buildStream(env)).thenReturn(stream2);

        // Now directly inject a mock ResourcesWrapper into the SpanStreamBuilder
        ResourcesWrapper wrapper = new ResourcesWrapper(Arrays.asList(resource1, resource2));
        SpanStreamBuilder builder = new SpanStreamBuilder(wrapper);

        DataStream<Span> unifiedStream = builder.unifiedStream(env);

        DataStream<String> output = unifiedStream.map(span ->
                span.getTraceId() + "|" + span.getSpan() + "|" + span.getService()
        );

        output.addSink(new SinkFunction<String>() {
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

        verify(resource1, times(1)).resolveProvider();
        verify(resource2, times(1)).resolveProvider();
        verify(provider1, times(1)).buildStream(env);
        verify(provider2, times(1)).buildStream(env);
    }
}
