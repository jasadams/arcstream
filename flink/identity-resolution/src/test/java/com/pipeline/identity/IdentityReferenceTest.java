package com.pipeline.identity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import static org.junit.Assert.*;

/** Runs the deployed function, including its merge side output, against a reviewed oracle. */
public class IdentityReferenceTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    private KeyedOneInputStreamOperatorTestHarness<String, RawEvent, UnifiedEvent> createHarness()
            throws Exception {
        KeyedOneInputStreamOperatorTestHarness<String, RawEvent, UnifiedEvent> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(new IdentityResolutionFunction()),
                        event -> event.tenantId, Types.STRING);
        harness.setup();
        return harness;
    }

    private InputStream resource(String name) {
        InputStream stream = getClass().getResourceAsStream("/reference/" + name);
        assertNotNull("Missing reference resource " + name, stream);
        return stream;
    }

    private ObjectNode record(String stream, Object payload) {
        ObjectNode result = JSON.createObjectNode();
        result.put("stream", stream);
        result.set("payload", JSON.valueToTree(payload));
        return result;
    }

    private void drain(KeyedOneInputStreamOperatorTestHarness<String, RawEvent, UnifiedEvent> harness,
                       List<JsonNode> captured) {
        for (UnifiedEvent event : harness.extractOutputValues()) {
            captured.add(record("unified-events", event));
        }
        harness.getOutput().clear();
        Queue<StreamRecord<MergeEvent>> merges = harness.getSideOutput(IdentityResolutionFunction.MERGE_TAG);
        if (merges != null) {
            StreamRecord<MergeEvent> merge;
            while ((merge = merges.poll()) != null) {
                captured.add(record("identity-merges", merge.getValue()));
            }
        }
    }

    private String canonical(Map<String, Map<String, String>> tenants, String tenant, String actual) {
        assertNotNull("Production must assign a canonical ID", actual);
        assertFalse(actual.isEmpty());
        Map<String, String> ids = tenants.computeIfAbsent(tenant, ignored -> new LinkedHashMap<>());
        return ids.computeIfAbsent(actual, ignored -> "c" + (ids.size() + 1));
    }

    /** Normalize only UUID allocation; keep fields, merge direction and record order observable. */
    private List<JsonNode> normalize(List<JsonNode> captured) {
        Map<String, Map<String, String>> ids = new HashMap<>();
        List<JsonNode> normalized = new ArrayList<>();
        for (JsonNode original : captured) {
            ObjectNode copy = original.deepCopy();
            ObjectNode payload = (ObjectNode) copy.get("payload");
            String tenant = payload.get("tenant_id").asText();
            if (copy.get("stream").asText().equals("unified-events")) {
                payload.put("canonical_id", canonical(ids, tenant, payload.get("canonical_id").asText()));
            } else {
                Map<String, String> tenantIds = ids.get(tenant);
                String oldId = payload.get("old_canonical_id").asText();
                String newId = payload.get("canonical_id").asText();
                assertNotNull("Merged tenant must already have unified events", tenantIds);
                assertTrue("Merge loser must have been observed", tenantIds.containsKey(oldId));
                assertTrue("Merge winner must have been observed", tenantIds.containsKey(newId));
                payload.put("old_canonical_id", tenantIds.get(oldId));
                payload.put("canonical_id", tenantIds.get(newId));
            }
            normalized.add(copy);
        }
        return normalized;
    }

    @Test
    public void identityLifecycleAndSnapshotRecoveryMatchReference() throws Exception {
        JsonNode fixture;
        try (InputStream input = resource("identity-input.json")) {
            fixture = JSON.readTree(input);
        }
        List<JsonNode> expected = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                resource("identity-expected.jsonl"), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isBlank()) expected.add(JSON.readTree(line));
            }
        }
        List<JsonNode> captured = new ArrayList<>();
        Map<String, JsonNode> inputs = new HashMap<>();
        KeyedOneInputStreamOperatorTestHarness<String, RawEvent, UnifiedEvent> harness = createHarness();
        harness.open();
        int restores = 0;
        try {
            for (JsonNode step : fixture.get("steps")) {
                if (step.get("op").asText().equals("snapshot_restore")) {
                    OperatorSubtaskState snapshot = harness.snapshot(step.get("checkpoint_id").asLong(), 0L);
                    harness.close();
                    harness = createHarness();
                    harness.initializeState(snapshot);
                    harness.open();
                    restores++;
                } else {
                    assertEquals("event", step.get("op").asText());
                    JsonNode payload = step.get("payload");
                    RawEvent event = JSON.treeToValue(payload, RawEvent.class);
                    inputs.put(event.eventId, payload);
                    harness.processElement(new StreamRecord<>(event));
                    drain(harness, captured);
                }
            }
        } finally {
            harness.close();
        }
        assertEquals("Fixture must exercise a reconstructed operator", 1, restores);
        assertEquals("Fixture must retain all outputs", 15, captured.size());
        Map<String, String> assignedIds = new HashMap<>();
        for (JsonNode output : captured) {
            if (output.get("stream").asText().equals("unified-events")) {
                assignedIds.put(output.get("payload").get("event_id").asText(),
                        output.get("payload").get("canonical_id").asText());
                ObjectNode forwarded = output.get("payload").deepCopy();
                forwarded.remove("canonical_id");
                assertEquals("Every raw field, including explicit nulls, must survive forwarding",
                        inputs.get(forwarded.get("event_id").asText()), forwarded);
            }
        }
        assertNotEquals("Identical anonymous/user identifiers in different tenants must remain distinct",
                assignedIds.get("identity-01"), assignedIds.get("identity-08"));
        assertEquals("Known-user linkage must survive reconstructing the operator",
                assignedIds.get("identity-01"), assignedIds.get("identity-13"));
        assertEquals("Independent expected links, merge direction, tenant isolation and restore continuity",
                expected, normalize(captured));
        Path destination = Path.of("target", "reference", "identity.jsonl");
        Files.createDirectories(destination.getParent());
        List<String> lines = new ArrayList<>();
        for (JsonNode output : captured) lines.add(JSON.writeValueAsString(output));
        Files.write(destination, lines, StandardCharsets.UTF_8);
    }
}
