package com.example.demo.config;

import com.example.demo.model.Span;
import com.example.demo.serialization.JsonSpanDeserializationSchema;
import com.example.demo.stream.SpanStreamProvider;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KafkaConfig implements SpanStreamProvider {

    private String topic;
    private String bootstrapServers;
    private String encoding;

    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }

    public String getBootstrapServers() { return bootstrapServers; }
    public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }

    public String getEncoding() { return encoding; }
    public void setEncoding(String encoding) { this.encoding = encoding; }

    @Override
    public DataStream<Span> buildStream(StreamExecutionEnvironment env) {
        DeserializationSchema<Span> schema = new JsonSpanDeserializationSchema(); // placeholder
        KafkaSource<Span> source = KafkaSource.<Span>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId("flink-" + topic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(schema)
                .build();

        return env.fromSource(source, WatermarkStrategy.noWatermarks(), topic);
    }
}
