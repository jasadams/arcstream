package com.pipeline.profile;

import com.pipeline.identity.UnifiedEvent;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class ProfileFunctionTest {

    private static final DateTimeFormatter FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    private KeyedOneInputStreamOperatorTestHarness<String, UnifiedEvent, ProfileUpdate> harness;
    // Anchor event times near "now" so clampEventTime (91-day window) never
    // rewrites them regardless of when the test runs.
    private final long base = System.currentTimeMillis() - 60 * 60 * 1000L;

    @Before
    public void setUp() throws Exception {
        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new ProfileFunction()),
                event -> event.canonicalId,
                Types.STRING);
        harness.open();
    }

    @After
    public void tearDown() throws Exception {
        harness.close();
    }

    private UnifiedEvent event(String sessionId, long atMs, String type, String page) {
        UnifiedEvent e = new UnifiedEvent();
        e.eventId = "evt-" + atMs;
        e.canonicalId = "user-1";
        e.tenantId = "acme-corp";
        e.eventTime = FMT.format(Instant.ofEpochMilli(atMs));
        e.eventType = type;
        e.sessionId = sessionId;
        e.pageUrl = page;
        e.deviceType = "desktop";
        e.browser = "Chrome";
        e.country = "US";
        return e;
    }

    private void send(UnifiedEvent e, long atMs) throws Exception {
        harness.processElement(new StreamRecord<>(e, atMs));
    }

    private List<ProfileUpdate> output() {
        return harness.extractOutputValues();
    }

    @Test
    public void firstEventEmitsCreateImmediately() throws Exception {
        send(event("sess-1", base, "page_view", "/dashboard"), base);

        List<ProfileUpdate> out = output();
        assertEquals(1, out.size());
        ProfileUpdate u = out.get(0);
        assertEquals("create", u.action);
        assertEquals(1, u.totalEvents);
        assertEquals(1, u.totalSessions);
        assertTrue(u.changedFields.contains("total_events"));
    }

    @Test
    public void subsequentEventsAreDebouncedIntoOneUpdate() throws Exception {
        harness.setProcessingTime(0);
        send(event("sess-1", base, "page_view", "/dashboard"), base);
        send(event("sess-1", base + 1000, "click", "/dashboard"), base + 1000);
        send(event("sess-1", base + 2000, "click", "/settings"), base + 2000);
        send(event("sess-1", base + 3000, "feature_used", "/settings"), base + 3000);

        // Only the create has been emitted so far
        assertEquals(1, output().size());

        harness.setProcessingTime(ProfileFunction.EMIT_DEBOUNCE_MS + 1);

        List<ProfileUpdate> out = output();
        assertEquals(2, out.size());
        ProfileUpdate u = out.get(1);
        assertEquals("update", u.action);
        assertEquals(4, u.totalEvents);
        assertEquals(2, u.clicks);
        assertEquals(1, u.featureUses);
        assertTrue(u.changedFields.contains("total_events"));
        assertTrue(u.changedFields.contains("clicks"));
        // Debounce timer is one-shot: nothing further without new events
        harness.setProcessingTime(ProfileFunction.EMIT_DEBOUNCE_MS * 10);
        assertEquals(2, output().size());
    }

    @Test
    public void debounceRearmsAfterFlush() throws Exception {
        harness.setProcessingTime(0);
        send(event("sess-1", base, "page_view", "/a"), base);
        send(event("sess-1", base + 1000, "click", "/a"), base + 1000);
        harness.setProcessingTime(ProfileFunction.EMIT_DEBOUNCE_MS + 1);
        assertEquals(2, output().size());

        send(event("sess-1", base + 2000, "click", "/b"), base + 2000);
        harness.setProcessingTime(2 * ProfileFunction.EMIT_DEBOUNCE_MS + 2);

        List<ProfileUpdate> out = output();
        assertEquals(3, out.size());
        assertEquals(3, out.get(2).totalEvents);
        assertEquals("/b", out.get(2).lastPage);
    }

    @Test
    public void sessionTransitionsCountDistinctSessions() throws Exception {
        harness.setProcessingTime(0);
        send(event("sess-1", base, "page_view", "/a"), base);
        send(event("sess-1", base + 1000, "click", "/a"), base + 1000);
        send(event("sess-2", base + 2000, "page_view", "/a"), base + 2000);
        send(event("sess-2", base + 3000, "click", "/a"), base + 3000);
        send(event("sess-3", base + 4000, "page_view", "/a"), base + 4000);
        harness.setProcessingTime(ProfileFunction.EMIT_DEBOUNCE_MS + 1);

        List<ProfileUpdate> out = output();
        ProfileUpdate u = out.get(out.size() - 1);
        assertEquals(3, u.totalSessions);
        // All sessions started today (same day window)
        assertEquals(3, u.sessions1d);
        assertEquals(3, u.sessions90d);
    }

    @Test
    public void sessionTimeoutEmitsImmediatelyAndClosesSession() throws Exception {
        harness.setProcessingTime(0);
        send(event("sess-1", base, "page_view", "/a"), base);
        assertEquals(1, output().size());

        // Advance event time past the 30-minute session gap
        harness.processWatermark(new Watermark(base + 31 * 60 * 1000L));

        List<ProfileUpdate> out = output();
        List<ProfileUpdate> timeouts = out.stream()
                .filter(p -> "session_timeout".equals(p.trigger))
                .collect(Collectors.toList());
        assertEquals(1, timeouts.size());
        assertFalse(timeouts.get(0).currentSessionActive);
        assertTrue(timeouts.get(0).avgSessionDurationSec >= 0);
    }

    @Test
    public void changedFieldsReflectDeltasSinceLastEmission() throws Exception {
        harness.setProcessingTime(0);
        send(event("sess-1", base, "page_view", "/a"), base);
        harness.setProcessingTime(ProfileFunction.EMIT_DEBOUNCE_MS + 1);

        // Only a country change in the second batch
        UnifiedEvent e = event("sess-1", base + 1000, "page_view", "/a");
        e.country = "DE";
        send(e, base + 1000);
        harness.setProcessingTime(2 * ProfileFunction.EMIT_DEBOUNCE_MS + 2);

        List<ProfileUpdate> out = output();
        ProfileUpdate last = out.get(out.size() - 1);
        assertTrue(last.changedFields.contains("last_country"));
        assertTrue(last.changedFields.contains("total_events"));
        assertFalse(last.changedFields.contains("clicks"));
        assertEquals("DE", last.lastCountry);
    }
}
