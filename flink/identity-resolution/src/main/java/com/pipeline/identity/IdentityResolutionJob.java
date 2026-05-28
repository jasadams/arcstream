package com.pipeline.identity;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.time.Duration;

public class IdentityResolutionJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String brokers = envOrDefault("KAFKA_BROKERS",
                "redpanda.data-pipeline.svc.cluster.local:9092");
        String inputTopic = envOrDefault("INPUT_TOPIC", "raw-events");
        String outputTopic = envOrDefault("OUTPUT_TOPIC", "unified-events");
        String mergesTopic = envOrDefault("MERGES_TOPIC", "identity-merges");
        String groupId = envOrDefault("GROUP_ID", "flink-identity-resolution");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000, CheckpointingMode.EXACTLY_ONCE);

        KafkaSource<RawEvent> source = KafkaSource.<RawEvent>builder()
                .setBootstrapServers(brokers)
                .setTopics(inputTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetsInitializer.earliest()))
                .setValueOnlyDeserializer(new AbstractDeserializationSchema<RawEvent>() {
                    @Override
                    public RawEvent deserialize(byte[] bytes) throws IOException {
                        return MAPPER.readValue(bytes, RawEvent.class);
                    }
                })
                .build();

        DataStream<RawEvent> rawEvents = env.fromSource(
                source,
                WatermarkStrategy.<RawEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withIdleness(Duration.ofSeconds(30)),
                "raw-events-source");

        SingleOutputStreamOperator<UnifiedEvent> unified = rawEvents
                .keyBy(event -> event.tenantId)
                .process(new IdentityResolutionFunction())
                .name("identity-resolution");

        // Main output: unified events
        KafkaSink<UnifiedEvent> unifiedSink = KafkaSink.<UnifiedEvent>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(outputTopic)
                        .setValueSerializationSchema(jsonSerializer(UnifiedEvent.class))
                        .build())
                .build();
        unified.sinkTo(unifiedSink).name("unified-events-sink");

        // Side output: merge events
        DataStream<MergeEvent> merges = unified
                .getSideOutput(IdentityResolutionFunction.MERGE_TAG);

        KafkaSink<MergeEvent> mergesSink = KafkaSink.<MergeEvent>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(mergesTopic)
                        .setValueSerializationSchema(jsonSerializer(MergeEvent.class))
                        .build())
                .build();
        merges.sinkTo(mergesSink).name("identity-merges-sink");

        env.execute("Identity Resolution");
    }

    private static <T> SerializationSchema<T> jsonSerializer(Class<T> clazz) {
        return new SerializationSchema<T>() {
            @Override
            public byte[] serialize(T element) {
                try {
                    return MAPPER.writeValueAsBytes(element);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to serialize " + clazz.getSimpleName(), e);
                }
            }
        };
    }

    private static String envOrDefault(String key, String defaultValue) {
        String val = System.getenv(key);
        return (val != null && !val.isEmpty()) ? val : defaultValue;
    }
}
