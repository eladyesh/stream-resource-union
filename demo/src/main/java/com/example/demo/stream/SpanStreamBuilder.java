package com.example.demo.stream;

import com.example.demo.config.ResourceConfigLoader;
import com.example.demo.config.SpanResource;
import com.example.demo.model.Span;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class SpanStreamBuilder {

    private final ResourceConfigLoader loader;

    public SpanStreamBuilder(ResourceConfigLoader loader) {
        this.loader = loader;
    }

    public DataStream<Span> unifiedStream(StreamExecutionEnvironment env) throws Exception {
        List<SpanResource> resources = loader.loadConfig("application.yaml").getResources();

        DataStream<Span> result = null;
        for (SpanResource res : resources) {
            SpanStreamProvider provider = res.resolveProvider();
            DataStream<Span> stream = provider.buildStream(env);
            result = (result == null) ? stream : result.union(stream);
        }

        return result;
    }
}
