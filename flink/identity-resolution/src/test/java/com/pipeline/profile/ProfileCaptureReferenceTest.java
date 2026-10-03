package com.pipeline.profile;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.pipeline.identity.UnifiedEvent;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.Test;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/** Captures the production operator. The independently authored oracle is compared outside Flink. */
public class ProfileCaptureReferenceTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final DateTimeFormatter FMT = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    private KeyedOneInputStreamOperatorTestHarness<String, UnifiedEvent, ProfileUpdate> harness()
            throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new ProfileFunction()), e -> e.canonicalId, Types.STRING);
    }

    @Test
    public void captureDebounceSessionAndDecayAfterFreshOperatorRecovery() throws Exception {
        ObjectNode fixture;
        try (InputStream input = getClass().getResourceAsStream("/reference/profile-input.json")) {
            assertNotNull(input);
            fixture = (ObjectNode) JSON.readTree(input);
        }
        long base = LocalDate.now(ZoneOffset.UTC).minusDays(1).atTime(12, 0)
                .toInstant(ZoneOffset.UTC).toEpochMilli();
        fixture.put("base_time_ms", base);
        ArrayNode steps = (ArrayNode) fixture.get("steps");
        List<JsonNode> captured = new ArrayList<>();
        KeyedOneInputStreamOperatorTestHarness<String, UnifiedEvent, ProfileUpdate> h = harness();
        h.open();
        int restores = 0;
        try {
            for (JsonNode raw : steps) {
                ObjectNode step = (ObjectNode) raw;
                String op = step.get("op").asText();
                long before = System.currentTimeMillis();
                switch (op) {
                    case "event" -> {
                        long time = base + step.get("offset_ms").asLong();
                        ObjectNode payload = (ObjectNode) step.get("payload");
                        payload.put("event_time", FMT.format(Instant.ofEpochMilli(time)));
                        step.put("event_time_ms", time);
                        UnifiedEvent event = JSON.treeToValue(payload, UnifiedEvent.class);
                        h.processElement(new StreamRecord<>(event, time));
                    }
                    case "processing_time" -> h.setProcessingTime(step.get("time_ms").asLong());
                    case "watermark" -> {
                        long time = base + step.get("offset_ms").asLong();
                        step.put("time_ms", time);
                        h.processWatermark(new Watermark(time));
                    }
                    case "snapshot_restore" -> {
                        OperatorSubtaskState snapshot = h.snapshot(step.get("checkpoint_id").asLong(), 0L);
                        h.close();
                        h = harness();
                        h.initializeState(snapshot);
                        h.open();
                        restores++;
                    }
                    default -> fail("Unknown fixture operation: " + op);
                }
                long after = System.currentTimeMillis();
                for (ProfileUpdate output : h.extractOutputValues()) {
                    ObjectNode row = JSON.createObjectNode();
                    row.put("stream", "profile-updates");
                    row.put("step", step.get("step").asText());
                    row.put("captured_from_ms", before);
                    row.put("captured_to_ms", after);
                    row.set("payload", JSON.valueToTree(output));
                    assertEquals("Capture every production field", 33, row.get("payload").size());
                    captured.add(row);
                }
                h.getOutput().clear();
            }
        } finally {
            h.close();
        }
        assertEquals(3, restores);
        assertEquals("One create, one debounced update, one timeout and three decays", 6, captured.size());
        assertEquals(List.of("create", "debounce", "timeout", "decay-1d", "decay-7d", "decay-30d"),
                captured.stream().map(row -> row.get("step").asText()).toList());
        assertEquals(List.of("event", "event", "session_timeout", "window_decay_1d",
                        "window_decay_7d", "window_decay_30d"),
                captured.stream().map(row -> row.get("payload").get("trigger").asText()).toList());
        JsonNode flushed = captured.get(1).get("payload");
        assertEquals(4, flushed.get("total_events").asLong());
        assertEquals(1, flushed.get("page_views").asLong());
        assertEquals(1, flushed.get("clicks").asLong());
        assertEquals(1, flushed.get("feature_uses").asLong());
        assertEquals(1, flushed.get("logins").asLong());
        assertEquals("known-user", flushed.get("user_id").asText());
        assertEquals(120, captured.get(2).get("payload").get("avg_session_duration_sec").asLong());
        assertEquals(0, captured.get(3).get("payload").get("events_1d").asLong());
        assertEquals(0, captured.get(4).get("payload").get("events_7d").asLong());
        assertEquals(0, captured.get(5).get("payload").get("events_30d").asLong());
        assertEquals(4, captured.get(5).get("payload").get("events_90d").asLong());
        Path directory = Path.of("target", "reference");
        Files.createDirectories(directory);
        StringBuilder lines = new StringBuilder();
        for (JsonNode row : captured) lines.append(JSON.writeValueAsString(row)).append('\n');
        Files.writeString(directory.resolve("profile.jsonl"), lines);
        Files.writeString(directory.resolve("profile-fixture.json"),
                JSON.writerWithDefaultPrettyPrinter().writeValueAsString(fixture) + "\n");
    }
}
