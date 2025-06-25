package com.example.demo.config;

import com.example.demo.model.Span;
import com.example.demo.stream.SpanStreamProvider;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.InputStream;
import java.util.List;

public class FileConfig implements SpanStreamProvider {

    private String path;

    public String getPath() { return path; }
    public void setPath(String path) { this.path = path; }

    @Override
    public DataStream<Span> buildStream(StreamExecutionEnvironment env) throws Exception {
        InputStream in = getClass().getClassLoader().getResourceAsStream(path);
        if (in == null) throw new IllegalArgumentException("File not found: " + path);
        List<Span> spans = new ObjectMapper().readValue(in, new TypeReference<>() {});
        return env.fromCollection(spans);
    }
}
