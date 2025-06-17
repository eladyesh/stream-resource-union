package com.example.demo.stream;

import com.example.demo.model.Span;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public interface SpanStreamProvider {
    DataStream<Span> buildStream(StreamExecutionEnvironment env) throws Exception;
}
