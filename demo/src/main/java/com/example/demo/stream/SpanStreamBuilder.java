package com.example.demo.stream;

import com.example.demo.config.ResolvedSpanResources;
import com.example.demo.config.SpanResource;
import com.example.demo.model.Span;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.stereotype.Component;

@Component
public class SpanStreamBuilder {

    private final ResolvedSpanResources config;

    public SpanStreamBuilder(ResolvedSpanResources config) {
        this.config = config;
    }

    public DataStream<Span> unifiedStream(StreamExecutionEnvironment env) throws Exception {
        DataStream<Span> result = null;

        for (SpanResource res : config.getResources()) {
            DataStream<Span> stream = res.resolveProvider().buildStream(env);
            result = (result == null) ? stream : result.union(stream);
        }

        return result;
    }
}
