package com.example.demo.stream;

import com.example.demo.config.KafkaConfig;
import com.example.demo.config.KafkaConfigLoader;
import com.example.demo.config.KafkaResource;
import com.example.demo.model.Span;
import com.example.demo.serialization.JsonSpanDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class KafkaSpanResource {

    private final KafkaConfigLoader configLoader;

    private final Map<String, DeserializationSchema<Span>> deserializerMap = new HashMap<>();

    public KafkaSpanResource(KafkaConfigLoader configLoader) {
        this.configLoader = configLoader;
        deserializerMap.put("json", new JsonSpanDeserializationSchema());
        deserializerMap.put("proto", new JsonSpanDeserializationSchema()); // placeholder
    }

    public DataStream<Span> unifiedKafkaSpanStream(StreamExecutionEnvironment env) throws Exception {
        var wrapper = configLoader.loadConfig("application.yaml");
        List<KafkaResource> resources = wrapper.getKafkaResources();

        DataStream<Span> unifiedStream = null;

        for (KafkaResource resource : resources) {
            KafkaConfig kafka = resource.getKafka();

            DeserializationSchema<Span> deserializer = deserializerMap.getOrDefault(
                    kafka.getEncoding() != null ? kafka.getEncoding().toLowerCase() : "json",
                    new JsonSpanDeserializationSchema()
            );

            String groupId = "flink-" + kafka.getTopic();

            KafkaSource<Span> source = KafkaSource.<Span>builder()
                    .setBootstrapServers(kafka.getBootstrapServers())
                    .setTopics(kafka.getTopic())
                    .setGroupId(groupId)
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(deserializer)
                    .build();

            DataStream<Span> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), kafka.getTopic());

            unifiedStream = (unifiedStream == null) ? stream : unifiedStream.union(stream);
        }

        return unifiedStream;
    }
}
