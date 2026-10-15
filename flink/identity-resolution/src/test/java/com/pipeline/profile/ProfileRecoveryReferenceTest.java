package com.pipeline.profile;

import com.pipeline.identity.UnifiedEvent;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static org.junit.Assert.*;

/** Uses yesterday at noon UTC to stay inside the production wall-clock 91-day clamp. */
public class ProfileRecoveryReferenceTest {
    private static final long DAY = 24 * 60 * 60 * 1000L;
    private static final long GAP = 30 * 60 * 1000L;
    private static final long BASE = LocalDate.now(ZoneOffset.UTC).minusDays(1)
            .atTime(12, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
    private static final DateTimeFormatter FMT = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    private KeyedOneInputStreamOperatorTestHarness<String, UnifiedEvent, ProfileUpdate> harness()
            throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new ProfileFunction()), e -> e.canonicalId, Types.STRING);
    }

    private UnifiedEvent event(String canonical, String tenant, long time, String type, String page) {
        UnifiedEvent e = new UnifiedEvent();
        e.eventId = "event-" + time;
        e.canonicalId = canonical;
        e.tenantId = tenant;
        e.userId = "known-user";
        e.sessionId = "session-1";
        e.eventTime = FMT.format(Instant.ofEpochMilli(time));
        e.eventType = type;
        e.pageUrl = page;
        e.deviceType = "desktop";
        e.browser = "Firefox";
        e.country = "AU";
        return e;
    }

    @Test
    public void freshHarnessRestoresDebounceAndSessionAndDecayTimers() throws Exception {
        OperatorSubtaskState snapshot;
        try (var h = harness()) {
            h.open();
            h.setProcessingTime(1_000);
            h.processElement(new StreamRecord<>(event("u", "a", BASE, "page_view", "/home"), BASE));
            UnifiedEvent click = event("u", "a", BASE + 120_000, "click", "/home");
            click.featureName = "search";
            h.processElement(new StreamRecord<>(click, BASE + 120_000));
            assertEquals(1, h.extractOutputValues().size());
            snapshot = h.snapshot(7, 1_001);
        }
        OperatorSubtaskState decaySnapshot;
        try (var restored = harness()) {
            restored.initializeState(snapshot);
            restored.open();
            restored.setProcessingTime(5_999);
            assertTrue(restored.extractOutputValues().isEmpty());
            restored.setProcessingTime(6_000);
            assertEquals(1, restored.extractOutputValues().size());
            ProfileUpdate update = restored.extractOutputValues().get(0);
            assertEquals("update", update.action);
            assertEquals("event", update.trigger);
            assertEquals("u", update.canonicalId);
            assertEquals("a", update.tenantId);
            assertEquals("known-user", update.userId);
            assertEquals(BASE, update.firstSeen);
            assertEquals(BASE + 120_000, update.lastSeen);
            assertEquals(2, update.totalEvents);
            assertEquals(1, update.totalSessions);
            assertEquals(1, update.pageViews);
            assertEquals(1, update.clicks);
            assertEquals(2, update.events1d);
            assertEquals(2, update.events90d);
            assertEquals(List.of("/home"), update.topPages);
            assertEquals(List.of("search"), update.topFeatures);
            assertEquals("AU", update.lastCountry);
            assertEquals("desktop", update.lastDevice);
            assertEquals("Firefox", update.lastBrowser);
            assertTrue(update.changedFields.contains("clicks"));
            assertTrue(update.currentSessionActive);
            restored.setProcessingTime(BASE + 10 * GAP);
            assertEquals("processing time must not close event-time sessions", 1,
                    restored.extractOutputValues().size());
            restored.processWatermark(new Watermark(BASE + GAP));
            assertEquals(1, restored.extractOutputValues().size());
            restored.processWatermark(new Watermark(BASE + 120_000 + GAP));
            assertEquals(2, restored.extractOutputValues().size());
            ProfileUpdate timeout = restored.extractOutputValues().get(1);
            assertEquals("session_timeout", timeout.trigger);
            assertFalse(timeout.currentSessionActive);
            assertEquals(120, timeout.avgSessionDurationSec);
            assertTrue(timeout.changedFields.contains("avg_session_duration_sec"));
            decaySnapshot = restored.snapshot(8, 6_001);
        }
        try (var restoredDecay = harness()) {
            restoredDecay.initializeState(decaySnapshot);
            restoredDecay.open();
            restoredDecay.processWatermark(new Watermark(BASE + 120_000 + DAY));
            assertEquals(1, restoredDecay.extractOutputValues().size());
            ProfileUpdate day = restoredDecay.extractOutputValues().get(0);
            assertEquals("window_decay_1d", day.trigger);
            assertEquals(0, day.events1d);
            assertEquals(2, day.events7d);
            assertEquals(0, day.sessions1d);
            assertEquals(1, day.sessions7d);
            restoredDecay.processWatermark(new Watermark(BASE + 120_000 + 7 * DAY));
            assertEquals(2, restoredDecay.extractOutputValues().size());
            ProfileUpdate week = restoredDecay.extractOutputValues().get(1);
            assertEquals("window_decay_7d", week.trigger);
            assertEquals(0, week.events7d);
            assertEquals(2, week.events30d);
            restoredDecay.processWatermark(new Watermark(BASE + 120_000 + 30 * DAY));
            assertEquals(3, restoredDecay.extractOutputValues().size());
            ProfileUpdate month = restoredDecay.extractOutputValues().get(2);
            assertEquals("window_decay_30d", month.trigger);
            assertEquals(0, month.events30d);
            assertEquals(2, month.events90d);
            assertEquals(2, month.totalEvents);
        }
    }

    @Test
    public void emptyMetadataRetainsLastNonemptyValuesAndCountsEveryEvent() throws Exception {
        try (var h = harness()) {
            h.open();
            h.setProcessingTime(0);
            h.processElement(new StreamRecord<>(event("u", "a", BASE, "page_view", "/home"), BASE));
            UnifiedEvent empty = event("u", "a", BASE + 1_000, "login", "");
            empty.userId = "";
            empty.country = "";
            empty.deviceType = null;
            empty.browser = "";
            h.processElement(new StreamRecord<>(empty, BASE + 1_000));
            h.setProcessingTime(ProfileFunction.EMIT_DEBOUNCE_MS);
            ProfileUpdate update = h.extractOutputValues().get(1);
            assertEquals(2, update.totalEvents);
            assertEquals(1, update.pageViews);
            assertEquals(1, update.logins);
            assertEquals("known-user", update.userId);
            assertEquals("/home", update.lastPage);
            assertEquals("AU", update.lastCountry);
            assertEquals("desktop", update.lastDevice);
            assertEquals("Firefox", update.lastBrowser);
            assertFalse(update.changedFields.contains("last_country"));
        }
    }

    @Test
    public void productionSelectorSeparatesCanonicalIdsButCollidesAcrossTenants() throws Exception {
        try (var h = harness()) {
            h.open();
            h.setProcessingTime(0);
            h.processElement(new StreamRecord<>(event("shared", "a", BASE, "page_view", "/a"), BASE));
            h.processElement(new StreamRecord<>(event("shared", "b", BASE + 1_000, "click", "/b"), BASE + 1_000));
            h.processElement(new StreamRecord<>(event("separate", "b", BASE, "page_view", "/other"), BASE));
            h.setProcessingTime(ProfileFunction.EMIT_DEBOUNCE_MS);
            assertEquals(3, h.extractOutputValues().size());
            ProfileUpdate shared = h.extractOutputValues().stream()
                    .filter(u -> u.canonicalId.equals("shared") && u.action.equals("update"))
                    .findFirst().orElseThrow();
            assertEquals("last event overwrites collided profile tenant", "b", shared.tenantId);
            assertEquals(2, shared.totalEvents);
            assertEquals(1, shared.pageViews);
            assertEquals(1, shared.clicks);
            ProfileUpdate separate = h.extractOutputValues().stream()
                    .filter(u -> u.canonicalId.equals("separate")).findFirst().orElseThrow();
            assertEquals(1, separate.totalEvents);
        }
    }

    @Test
    public void outOfOrderEventRetainsLastSeenButMovesTimeoutEarlier() throws Exception {
        try (var h = harness()) {
            h.open();
            h.processElement(new StreamRecord<>(event("u", "a", BASE + 120_000, "page_view", "/newer"), BASE + 120_000));
            h.processElement(new StreamRecord<>(event("u", "a", BASE, "click", "/older"), BASE));
            h.processWatermark(new Watermark(BASE + GAP));
            List<ProfileUpdate> timeouts = h.extractOutputValues().stream()
                    .filter(u -> u.trigger.equals("session_timeout")).toList();
            assertEquals("current function arms timeout from incoming event, not lastSeen", 1, timeouts.size());
            ProfileUpdate timeout = timeouts.get(0);
            assertEquals(BASE + 120_000, timeout.lastSeen);
            assertEquals(BASE + 120_000, timeout.firstSeen);
            assertEquals(2, timeout.totalEvents);
            assertEquals("/older", timeout.lastPage);
            assertFalse(timeout.currentSessionActive);
        }
    }
}
