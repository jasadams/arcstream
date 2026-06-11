package com.pipeline.session;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pipeline.identity.UnifiedEvent;
import com.pipeline.common.Checkpointing;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.time.Duration;

public class SessionizationJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String brokers = envOrDefault("KAFKA_BROKERS",
                "redpanda.data-pipeline.svc.cluster.local:9092");
        String inputTopic = envOrDefault("INPUT_TOPIC", "unified-events");
        String outputTopic = envOrDefault("OUTPUT_TOPIC", "session-events");
        String groupId = envOrDefault("GROUP_ID", "flink-sessionization");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Checkpointing.configure(env);

        KafkaSource<UnifiedEvent> source = KafkaSource.<UnifiedEvent>builder()
                .setBootstrapServers(brokers)
                .setTopics(inputTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new AbstractDeserializationSchema<UnifiedEvent>() {
                    @Override
                    public UnifiedEvent deserialize(byte[] bytes) throws IOException {
                        return MAPPER.readValue(bytes, UnifiedEvent.class);
                    }
                })
                .build();

        DataStream<UnifiedEvent> events = env.fromSource(
                source,
                WatermarkStrategy.<UnifiedEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withIdleness(Duration.ofSeconds(30)),
                "unified-events-source");

        DataStream<SessionSummary> sessions = events
                .keyBy(event -> event.sessionId)
                .process(new SessionFunction())
                .name("sessionization");

        KafkaSink<SessionSummary> sink = KafkaSink.<SessionSummary>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(outputTopic)
                        .setValueSerializationSchema((SerializationSchema<SessionSummary>) element -> {
                            try {
                                return MAPPER.writeValueAsBytes(element);
                            } catch (Exception e) {
                                throw new RuntimeException("Failed to serialize SessionSummary", e);
                            }
                        })
                        .build())
                .build();

        sessions.sinkTo(sink).name("session-events-sink");

        env.execute("Sessionization");
    }

    private static String envOrDefault(String key, String defaultValue) {
        String val = System.getenv(key);
        return (val != null && !val.isEmpty()) ? val : defaultValue;
    }
}
