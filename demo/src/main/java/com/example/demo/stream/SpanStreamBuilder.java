package com.example.demo.stream;

import com.example.demo.config.ResolvedSpanResources;
import com.example.demo.config.SpanResource;
import com.example.demo.model.Span;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.stereotype.Component;

@Component
public class SpanStreamBuilder {

    private final ResolvedSpanResources resolvedSpanResources;

    public SpanStreamBuilder(ResolvedSpanResources resolvedSpanResources) {
        this.resolvedSpanResources = resolvedSpanResources;
    }

    public DataStream<Span> buildUnifiedSpanStream(StreamExecutionEnvironment executionEnvironment) throws Exception {
        DataStream<Span> unifiedSpanStream = null;

        for (SpanResource spanResource : resolvedSpanResources.getResources()) {
            DataStream<Span> individualSpanStream = spanResource.resolveProvider().buildStream(executionEnvironment);
            unifiedSpanStream = (unifiedSpanStream == null)
                    ? individualSpanStream
                    : unifiedSpanStream.union(individualSpanStream);
        }

        return unifiedSpanStream;
    }
}
