package com.pipeline.profile;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pipeline.identity.UnifiedEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class ProfileUpdaterJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String brokers = envOrDefault("KAFKA_BROKERS",
                "redpanda.data-pipeline.svc.cluster.local:9092");
        String inputTopic = envOrDefault("INPUT_TOPIC", "unified-events");
        String outputTopic = envOrDefault("PROFILE_UPDATES_TOPIC", "profile-updates");
        String groupId = envOrDefault("GROUP_ID", "flink-profile-updater");
        String scyllaContactPoints = envOrDefault("SCYLLA_CONTACT_POINTS",
                "scylladb.data-pipeline.svc.cluster.local:9042");
        String scyllaKeyspace = envOrDefault("SCYLLA_KEYSPACE", "cdp");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000, CheckpointingMode.EXACTLY_ONCE);

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
                        .withTimestampAssigner((event, ts) -> clampEventTime(parseEventTime(event.eventTime)))
                        .withIdleness(Duration.ofSeconds(30)),
                "unified-events-source");

        DataStream<ProfileUpdate> profileUpdates = events
                .keyBy(event -> event.canonicalId)
                .process(new ProfileFunction(scyllaContactPoints, scyllaKeyspace))
                .name("profile-updater");

        KafkaSink<ProfileUpdate> profileSink = KafkaSink.<ProfileUpdate>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.<ProfileUpdate>builder()
                        .setTopic(outputTopic)
                        .setKeySerializationSchema(
                                (SerializationSchema<ProfileUpdate>) update ->
                                        update.canonicalId.getBytes(StandardCharsets.UTF_8))
                        .setValueSerializationSchema(
                                (SerializationSchema<ProfileUpdate>) update -> {
                                    try {
                                        return MAPPER.writeValueAsBytes(update);
                                    } catch (Exception e) {
                                        throw new RuntimeException(
                                                "Failed to serialize ProfileUpdate", e);
                                    }
                                })
                        .build())
                .build();

        profileUpdates.sinkTo(profileSink).name("profile-updates-sink");

        env.execute("Profile Updater");
    }

    private static final long MAX_PAST_MS = 7 * 24 * 60 * 60 * 1000L;
    private static final long MAX_FUTURE_MS = 60 * 1000L;

    private static long clampEventTime(long eventTimeMs) {
        long now = System.currentTimeMillis();
        if (eventTimeMs > now + MAX_FUTURE_MS) return now;
        if (eventTimeMs < now - MAX_PAST_MS) return now;
        return eventTimeMs;
    }

    private static long parseEventTime(String ts) {
        try {
            return java.time.LocalDateTime.parse(ts,
                    java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"))
                    .toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
        } catch (Exception e) {
            try {
                return java.time.LocalDateTime.parse(ts,
                        java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SS"))
                        .toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
            } catch (Exception e2) {
                return System.currentTimeMillis();
            }
        }
    }

    private static String envOrDefault(String key, String defaultValue) {
        String val = System.getenv(key);
        return (val != null && !val.isEmpty()) ? val : defaultValue;
    }
}
