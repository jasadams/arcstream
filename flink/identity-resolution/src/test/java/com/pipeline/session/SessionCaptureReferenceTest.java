package com.pipeline.session;

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
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/** Runs production sessionId-only semantics; the independent oracle is compared outside Flink. */
public class SessionCaptureReferenceTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final DateTimeFormatter FMT = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    private KeyedOneInputStreamOperatorTestHarness<String, UnifiedEvent, SessionSummary> harness()
            throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new SessionFunction()), e -> e.sessionId, Types.STRING);
    }

    @Test
    public void captureRecoveredDeadlinesAndFreshStateAfterClosedSessionReuse() throws Exception {
        ObjectNode fixture;
        try (InputStream input = getClass().getResourceAsStream("/reference/session-input.json")) {
            assertNotNull(input);
            fixture = (ObjectNode) JSON.readTree(input);
        }
        long base = Instant.parse(fixture.get("base_time").asText()).toEpochMilli();
        fixture.put("base_time_ms", base);
        List<JsonNode> captured = new ArrayList<>();
        KeyedOneInputStreamOperatorTestHarness<String, UnifiedEvent, SessionSummary> h = harness();
        h.open();
        int restores = 0;
        try {
            for (JsonNode raw : (ArrayNode) fixture.get("steps")) {
                ObjectNode step = (ObjectNode) raw;
                switch (step.get("op").asText()) {
                    case "event" -> {
                        long time = base + step.get("offset_ms").asLong();
                        ObjectNode payload = (ObjectNode) step.get("payload");
                        payload.put("event_time", FMT.format(Instant.ofEpochMilli(time)));
                        step.put("event_time_ms", time);
                        h.processElement(new StreamRecord<>(JSON.treeToValue(payload, UnifiedEvent.class), time));
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
                    default -> fail("Unknown fixture operation: " + step.get("op").asText());
                }
                for (SessionSummary output : h.extractOutputValues()) {
                    ObjectNode row = JSON.createObjectNode();
                    row.put("stream", "session-summaries");
                    row.put("step", step.get("step").asText());
                    row.set("payload", JSON.valueToTree(output));
                    assertEquals("Capture every production field", 12, row.get("payload").size());
                    captured.add(row);
                }
                h.getOutput().clear();
            }
        } finally {
            h.close();
        }
        assertEquals(3, restores);
        assertEquals("Two initial sessions and one fresh reused session", 3, captured.size());
        assertEquals(List.of("close-a", "close-b", "close-reused-a"),
                captured.stream().map(row -> row.get("step").asText()).toList());
        assertEquals(List.of(4, 2, 1),
                captured.stream().map(row -> row.get("payload").get("event_count").asInt()).toList());
        assertEquals(List.of(60L, 600L, 0L),
                captured.stream().map(row -> row.get("payload").get("duration_sec").asLong()).toList());
        Path directory = Path.of("target", "reference");
        Files.createDirectories(directory);
        StringBuilder lines = new StringBuilder();
        for (JsonNode row : captured) lines.append(JSON.writeValueAsString(row)).append('\n');
        Files.writeString(directory.resolve("session.jsonl"), lines);
        Files.writeString(directory.resolve("session-fixture.json"),
                JSON.writerWithDefaultPrettyPrinter().writeValueAsString(fixture) + "\n");
    }
}
