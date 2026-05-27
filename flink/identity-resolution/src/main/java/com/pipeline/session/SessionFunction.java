package com.pipeline.session;

import com.pipeline.identity.UnifiedEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class SessionFunction
        extends KeyedProcessFunction<String, UnifiedEvent, SessionSummary> {

    private static final long SESSION_TIMEOUT_MS = 30 * 60 * 1000L;

    private transient ValueState<SessionState> sessionState;

    @Override
    public void open(Configuration parameters) {
        sessionState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("session", SessionState.class));
    }

    @Override
    public void processElement(UnifiedEvent event, Context ctx, Collector<SessionSummary> out)
            throws Exception {

        SessionState session = sessionState.value();
        long eventTimeMs = parseTimestamp(event.eventTime);

        if (session == null) {
            session = new SessionState();
            session.sessionId = event.sessionId;
            session.canonicalId = event.canonicalId;
            session.tenantId = event.tenantId;
            session.startTimeMs = eventTimeMs;
        }

        session.lastEventTimeMs = Math.max(session.lastEventTimeMs, eventTimeMs);
        session.eventCount++;

        if (event.pageUrl != null && !event.pageUrl.isEmpty()) {
            session.pages.add(event.pageUrl);
        }
        session.eventTypeCounts.merge(event.eventType, 1, Integer::sum);

        if (event.deviceType != null && !event.deviceType.isEmpty()) {
            session.deviceType = event.deviceType;
        }
        if (event.browser != null && !event.browser.isEmpty()) {
            session.browser = event.browser;
        }
        if (event.country != null && !event.country.isEmpty()) {
            session.country = event.country;
        }

        // Register/reset session timeout timer
        long timerTime = ctx.timerService().currentProcessingTime() + SESSION_TIMEOUT_MS;
        ctx.timerService().registerProcessingTimeTimer(timerTime);

        sessionState.update(session);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<SessionSummary> out)
            throws Exception {

        SessionState session = sessionState.value();
        if (session == null) return;

        // Check if enough time has passed since last event
        long elapsed = System.currentTimeMillis() - session.lastEventTimeMs;
        if (elapsed >= SESSION_TIMEOUT_MS) {
            out.collect(buildSummary(session));
            sessionState.clear();
        } else {
            // Not timed out yet — re-register for remaining time
            long remaining = SESSION_TIMEOUT_MS - elapsed;
            ctx.timerService().registerProcessingTimeTimer(
                    ctx.timerService().currentProcessingTime() + remaining);
        }
    }

    private SessionSummary buildSummary(SessionState session) {
        SessionSummary s = new SessionSummary();
        s.sessionId = session.sessionId;
        s.canonicalId = session.canonicalId;
        s.tenantId = session.tenantId;
        s.startTime = formatTimestamp(session.startTimeMs);
        s.endTime = formatTimestamp(session.lastEventTimeMs);
        s.durationSec = (session.lastEventTimeMs - session.startTimeMs) / 1000;
        s.eventCount = session.eventCount;
        s.pages = new ArrayList<>(session.pages);
        s.eventTypes = session.eventTypeCounts;
        s.deviceType = session.deviceType;
        s.browser = session.browser;
        s.country = session.country;
        return s;
    }

    private long parseTimestamp(String ts) {
        try {
            return java.time.LocalDateTime.parse(ts,
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"))
                    .toInstant(ZoneOffset.UTC).toEpochMilli();
        } catch (Exception e) {
            try {
                return java.time.LocalDateTime.parse(ts,
                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SS"))
                        .toInstant(ZoneOffset.UTC).toEpochMilli();
            } catch (Exception e2) {
                return System.currentTimeMillis();
            }
        }
    }

    private String formatTimestamp(long epochMs) {
        return Instant.ofEpochMilli(epochMs)
                .atOffset(ZoneOffset.UTC)
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
    }
}
