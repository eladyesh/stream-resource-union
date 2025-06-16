package com.example.demo.stream;

import com.example.demo.config.KafkaConfigLoader;
import com.example.demo.config.KafkaResource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class KafkaSpanResource {

    private final KafkaConfigLoader configLoader;

    public KafkaSpanResource(KafkaConfigLoader configLoader) {
        this.configLoader = configLoader;
    }

    public DataStream<String> unifiedKafkaSpanStream(StreamExecutionEnvironment env) throws Exception {
        var wrapper = configLoader.loadConfig("application.yaml");  // <-- loads from classpath now
        List<KafkaResource> resources = wrapper.getKafkaResources();

        DataStream<String> unifiedStream = null;

        for (KafkaResource resource : resources) {
            var kafka = resource.getKafka();

            KafkaSource<String> source = KafkaSource.<String>builder()
                    .setBootstrapServers(kafka.getBootstrapServers())
                    .setTopics(kafka.getTopic())
                    .setGroupId("flink-consumer-group")
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .build();

            DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), kafka.getTopic());

            unifiedStream = (unifiedStream == null) ? stream : unifiedStream.union(stream);
        }

        return unifiedStream;
    }
}
