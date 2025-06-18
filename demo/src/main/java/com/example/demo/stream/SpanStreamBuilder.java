package com.example.demo.stream;

import com.example.demo.config.ResourcesWrapper;
import com.example.demo.config.SpanResource;
import com.example.demo.model.Span;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class SpanStreamBuilder {

    private final ResourcesWrapper config;

    public SpanStreamBuilder(ResourcesWrapper config) {
        this.config = config;
    }

    public DataStream<Span> unifiedStream(StreamExecutionEnvironment env) throws Exception {
        List<SpanResource> resources = config.getResources();

        DataStream<Span> result = null;
        for (SpanResource res : resources) {
            DataStream<Span> stream = res.resolveProvider().buildStream(env);
            result = (result == null) ? stream : result.union(stream);
        }

        return result;
    }
}
