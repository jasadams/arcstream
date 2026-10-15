package com.pipeline.session;

import com.pipeline.identity.UnifiedEvent;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Set;

import static org.junit.Assert.*;

/** Reference semantics of the production sessionId-only selector and SessionFunction. */
public class SessionReferenceTest {
    private static final long BASE = Instant.parse("2026-06-01T12:00:00Z").toEpochMilli();
    private static final long GAP = 30 * 60 * 1000L;
    private static final DateTimeFormatter FMT = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    private KeyedOneInputStreamOperatorTestHarness<String, UnifiedEvent, SessionSummary> harness()
            throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new SessionFunction()), e -> e.sessionId, Types.STRING);
    }

    private UnifiedEvent event(String session, String tenant, long time, String type, String page) {
        UnifiedEvent e = new UnifiedEvent();
        e.eventId = "event-" + time;
        e.canonicalId = "canonical-" + tenant;
        e.sessionId = session;
        e.tenantId = tenant;
        e.eventTime = FMT.format(Instant.ofEpochMilli(time));
        e.eventType = type;
        e.pageUrl = page;
        e.deviceType = "desktop";
        e.browser = "Firefox";
        e.country = "AU";
        return e;
    }

    @Test
    public void closesAtExtendedEventDeadlineAndIgnoresProcessingClock() throws Exception {
        try (var h = harness()) {
            h.open();
            h.processElement(new StreamRecord<>(event("s", "a", BASE, "page_view", "/home"), BASE));
            h.processElement(new StreamRecord<>(event("s", "a", BASE + 60_000, "click", "/home"), BASE + 60_000));
            h.setProcessingTime(BASE + 10 * GAP);
            assertTrue(h.extractOutputValues().isEmpty());
            h.processWatermark(new Watermark(BASE + GAP));
            assertTrue("superseded deadline must not close the session", h.extractOutputValues().isEmpty());
            h.processWatermark(new Watermark(BASE + 60_000 + GAP - 1));
            assertTrue(h.extractOutputValues().isEmpty());
            h.processWatermark(new Watermark(BASE + 60_000 + GAP));
            assertEquals(1, h.extractOutputValues().size());
            SessionSummary s = h.extractOutputValues().get(0);
            assertEquals("s", s.sessionId);
            assertEquals("canonical-a", s.canonicalId);
            assertEquals("a", s.tenantId);
            assertEquals(2, s.eventCount);
            assertEquals(60, s.durationSec);
            assertEquals(Set.of("/home"), Set.copyOf(s.pages));
            assertEquals(Integer.valueOf(1), s.eventTypes.get("page_view"));
            assertEquals(Integer.valueOf(1), s.eventTypes.get("click"));
            assertEquals("desktop", s.deviceType);
            assertEquals("Firefox", s.browser);
            assertEquals("AU", s.country);
            h.processWatermark(new Watermark(BASE + 20 * GAP));
            assertEquals("closed state must not emit twice", 1, h.extractOutputValues().size());
        }
    }

    @Test
    public void freshHarnessRestoresStateAndExtendedEventTimeTimer() throws Exception {
        OperatorSubtaskState snapshot;
        try (var h = harness()) {
            h.open();
            h.processElement(new StreamRecord<>(event("s", "a", BASE, "page_view", "/one"), BASE));
            h.processElement(new StreamRecord<>(event("s", "a", BASE + 120_000, "click", "/two"), BASE + 120_000));
            snapshot = h.snapshot(7, 1_000);
        }
        try (var restored = harness()) {
            restored.initializeState(snapshot);
            restored.open();
            restored.processWatermark(new Watermark(BASE + GAP));
            assertTrue(restored.extractOutputValues().isEmpty());
            restored.processWatermark(new Watermark(BASE + 120_000 + GAP));
            assertEquals(1, restored.extractOutputValues().size());
            SessionSummary s = restored.extractOutputValues().get(0);
            assertEquals(2, s.eventCount);
            assertEquals(120, s.durationSec);
            assertEquals(Set.of("/one", "/two"), Set.copyOf(s.pages));
            assertEquals(FMT.format(Instant.ofEpochMilli(BASE)), s.startTime);
            assertEquals(FMT.format(Instant.ofEpochMilli(BASE + 120_000)), s.endTime);
        }
    }

    @Test
    public void outOfOrderInputKeepsFirstArrivalStartAndMaximumEnd() throws Exception {
        try (var h = harness()) {
            h.open();
            h.processElement(new StreamRecord<>(event("s", "a", BASE + 60_000, "click", "/newer"), BASE + 60_000));
            h.processElement(new StreamRecord<>(event("s", "a", BASE, "page_view", "/older"), BASE));
            h.processWatermark(new Watermark(BASE + GAP));
            assertTrue(h.extractOutputValues().isEmpty());
            h.processWatermark(new Watermark(BASE + 60_000 + GAP));
            SessionSummary s = h.extractOutputValues().get(0);
            assertEquals(2, s.eventCount);
            assertEquals(FMT.format(Instant.ofEpochMilli(BASE + 60_000)), s.startTime);
            assertEquals(s.startTime, s.endTime);
            assertEquals(0, s.durationSec);
        }
    }

    @Test
    public void productionSelectorSeparatesSessionIdsButCollidesAcrossTenants() throws Exception {
        try (var h = harness()) {
            h.open();
            h.processElement(new StreamRecord<>(event("shared", "a", BASE, "page_view", "/a"), BASE));
            h.processElement(new StreamRecord<>(event("shared", "b", BASE, "click", "/b"), BASE));
            h.processElement(new StreamRecord<>(event("separate", "b", BASE, "page_view", "/other"), BASE));
            h.processWatermark(new Watermark(BASE + GAP));
            assertEquals(2, h.extractOutputValues().size());
            SessionSummary shared = h.extractOutputValues().stream()
                    .filter(s -> s.sessionId.equals("shared")).findFirst().orElseThrow();
            assertEquals("first event owns the collided session metadata", "a", shared.tenantId);
            assertEquals("canonical-a", shared.canonicalId);
            assertEquals(2, shared.eventCount);
            assertEquals(Set.of("/a", "/b"), Set.copyOf(shared.pages));
            SessionSummary separate = h.extractOutputValues().stream()
                    .filter(s -> s.sessionId.equals("separate")).findFirst().orElseThrow();
            assertEquals("b", separate.tenantId);
            assertEquals(1, separate.eventCount);
        }
    }
}
