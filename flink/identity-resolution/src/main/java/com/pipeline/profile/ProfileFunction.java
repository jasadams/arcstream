package com.pipeline.profile;

import com.pipeline.identity.UnifiedEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class ProfileFunction
        extends KeyedProcessFunction<String, UnifiedEvent, ProfileUpdate> {

    private static final long SESSION_TIMEOUT_MS = 30 * 60 * 1000L;
    private static final long DAY_MS = 24 * 60 * 60 * 1000L;
    private static final long MAX_PAST_MS = 91 * DAY_MS;
    private static final long MAX_FUTURE_MS = 60 * 1000L;
    private static final int BUCKET_RETENTION_DAYS = 91;

    private transient ValueState<UserProfileState> profileState;

    public ProfileFunction() {
    }

    @Override
    public void open(Configuration parameters) {
        profileState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("user-profile", UserProfileState.class));
    }

    private long clampEventTime(long eventTimeMs) {
        long now = System.currentTimeMillis();
        if (eventTimeMs > now + MAX_FUTURE_MS) return now;
        if (eventTimeMs < now - MAX_PAST_MS) return now;
        return eventTimeMs;
    }

    @Override
    public void processElement(UnifiedEvent event, Context ctx, Collector<ProfileUpdate> out)
            throws Exception {

        UserProfileState profile = profileState.value();
        boolean isNewProfile = (profile == null);
        if (isNewProfile) {
            profile = new UserProfileState();
            profile.canonicalId = event.canonicalId;
            profile.tenantId = event.tenantId;
            profile.firstSeenEpochMs = clampEventTime(parseTimestamp(event.eventTime));
        }

        long eventTimeMs = clampEventTime(parseTimestamp(event.eventTime));

        if (event.userId != null && !event.userId.isEmpty()) {
            profile.userId = event.userId;
        }
        profile.tenantId = event.tenantId;
        profile.lastSeenEpochMs = Math.max(profile.lastSeenEpochMs, eventTimeMs);
        profile.totalEvents++;

        switch (event.eventType) {
            case "page_view" -> profile.pageViews++;
            case "click" -> profile.clicks++;
            case "signup" -> profile.signups++;
            case "login" -> profile.logins++;
            case "feature_used" -> profile.featureUses++;
        }

        String dateKey = Instant.ofEpochMilli(eventTimeMs)
                .atOffset(ZoneOffset.UTC).toLocalDate()
                .format(DateTimeFormatter.ISO_LOCAL_DATE);
        profile.dailyBuckets.merge(dateKey, 1L, Long::sum);

        if (event.sessionId != null && !event.sessionId.isEmpty()) {
            profile.dailySessions
                    .computeIfAbsent(dateKey, k -> new java.util.HashSet<>())
                    .add(event.sessionId);
        }
        pruneBuckets(profile);

        if (event.sessionId != null && !event.sessionId.isEmpty()) {
            profile.allSessionIds.add(event.sessionId);
            if (!event.sessionId.equals(profile.currentSessionId)) {
                profile.currentSessionId = event.sessionId;
                profile.currentSessionStartMs = eventTimeMs;
            }
        }

        if (event.pageUrl != null && !event.pageUrl.isEmpty()) {
            profile.lastPage = event.pageUrl;
            profile.pageCounts.merge(event.pageUrl, 1L, Long::sum);
        }
        if (event.country != null && !event.country.isEmpty()) {
            profile.lastCountry = event.country;
        }
        if (event.deviceType != null && !event.deviceType.isEmpty()) {
            profile.lastDevice = event.deviceType;
        }
        if (event.browser != null && !event.browser.isEmpty()) {
            profile.lastBrowser = event.browser;
        }
        if (event.featureName != null && !event.featureName.isEmpty()) {
            profile.featureCounts.merge(event.featureName, 1L, Long::sum);
        }

        // Cancel pending timers — user is active again
        if (profile.sessionTimer > 0) {
            ctx.timerService().deleteEventTimeTimer(profile.sessionTimer);
        }
        if (profile.decayTimer1d > 0) {
            ctx.timerService().deleteEventTimeTimer(profile.decayTimer1d);
            profile.decayTimer1d = 0;
        }
        if (profile.decayTimer7d > 0) {
            ctx.timerService().deleteEventTimeTimer(profile.decayTimer7d);
            profile.decayTimer7d = 0;
        }
        if (profile.decayTimer30d > 0) {
            ctx.timerService().deleteEventTimeTimer(profile.decayTimer30d);
            profile.decayTimer30d = 0;
        }

        // Schedule session timeout relative to event time
        profile.sessionTimer = eventTimeMs + SESSION_TIMEOUT_MS;
        ctx.timerService().registerEventTimeTimer(profile.sessionTimer);

        LocalDate eventDate = Instant.ofEpochMilli(eventTimeMs).atOffset(ZoneOffset.UTC).toLocalDate();
        ProfileUpdate before = isNewProfile ? null : buildProfileUpdate(profile, "update", "event", eventDate);

        profileState.update(profile);

        ProfileUpdate after = buildProfileUpdate(profile, isNewProfile ? "create" : "update", "event", eventDate);
        after.changedFields = computeChangedFields(before, after);
        out.collect(after);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<ProfileUpdate> out)
            throws Exception {

        UserProfileState profile = profileState.value();
        if (profile == null) return;

        LocalDate timerDate = Instant.ofEpochMilli(timestamp).atOffset(ZoneOffset.UTC).toLocalDate();

        if (timestamp == profile.sessionTimer && profile.currentSessionId != null) {
            ProfileUpdate before = buildProfileUpdate(profile, "update", "session_timeout", timerDate);

            long durationMs = profile.lastSeenEpochMs - profile.currentSessionStartMs;
            profile.closedSessionCount++;
            profile.totalSessionDurationMs += Math.max(0, durationMs);
            profile.currentSessionId = null;
            profile.currentSessionStartMs = 0;
            profile.sessionTimer = 0;

            long lastSeen = profile.lastSeenEpochMs;
            profile.decayTimer1d = lastSeen + DAY_MS;
            profile.decayTimer7d = lastSeen + (7 * DAY_MS);
            profile.decayTimer30d = lastSeen + (30 * DAY_MS);
            ctx.timerService().registerEventTimeTimer(profile.decayTimer1d);
            ctx.timerService().registerEventTimeTimer(profile.decayTimer7d);
            ctx.timerService().registerEventTimeTimer(profile.decayTimer30d);

            profileState.update(profile);
            ProfileUpdate after = buildProfileUpdate(profile, "update", "session_timeout", timerDate);
            after.changedFields = computeChangedFields(before, after);
            out.collect(after);
        } else if (timestamp == profile.decayTimer1d
                || timestamp == profile.decayTimer7d
                || timestamp == profile.decayTimer30d) {
            String trigger = timestamp == profile.decayTimer1d ? "window_decay_1d"
                    : timestamp == profile.decayTimer7d ? "window_decay_7d"
                    : "window_decay_30d";

            if (timestamp == profile.decayTimer1d) profile.decayTimer1d = 0;
            if (timestamp == profile.decayTimer7d) profile.decayTimer7d = 0;
            if (timestamp == profile.decayTimer30d) profile.decayTimer30d = 0;

            profileState.update(profile);
            ProfileUpdate update = buildProfileUpdate(profile, "update", trigger, timerDate);
            out.collect(update);
        }
    }

    private ProfileUpdate buildProfileUpdate(UserProfileState p, String action, String trigger, LocalDate referenceDate) {
        ProfileUpdate update = new ProfileUpdate();
        update.canonicalId = p.canonicalId;
        update.tenantId = p.tenantId;
        update.userId = p.userId != null ? p.userId : "";
        update.firstSeen = p.firstSeenEpochMs;
        update.lastSeen = p.lastSeenEpochMs;
        update.updatedAt = System.currentTimeMillis();
        update.totalEvents = p.totalEvents;
        update.totalSessions = p.allSessionIds.size();
        update.events1d = sumBuckets(p.dailyBuckets, referenceDate, 1);
        update.events7d = sumBuckets(p.dailyBuckets, referenceDate, 7);
        update.events30d = sumBuckets(p.dailyBuckets, referenceDate, 30);
        update.events90d = sumBuckets(p.dailyBuckets, referenceDate, 90);
        update.sessions1d = sumSessions(p.dailySessions, referenceDate, 1);
        update.sessions7d = sumSessions(p.dailySessions, referenceDate, 7);
        update.sessions30d = sumSessions(p.dailySessions, referenceDate, 30);
        update.sessions90d = sumSessions(p.dailySessions, referenceDate, 90);
        update.avgSessionDurationSec = p.closedSessionCount > 0
                ? p.totalSessionDurationMs / p.closedSessionCount / 1000 : 0;
        update.currentSessionActive = p.currentSessionId != null;
        update.currentSessionDurationSec = p.currentSessionId != null && p.currentSessionStartMs > 0
                ? (System.currentTimeMillis() - p.currentSessionStartMs) / 1000 : 0;
        update.pageViews = p.pageViews;
        update.clicks = p.clicks;
        update.logins = p.logins;
        update.featureUses = p.featureUses;
        update.lastPage = p.lastPage != null ? p.lastPage : "";
        update.lastCountry = p.lastCountry != null ? p.lastCountry : "";
        update.lastDevice = p.lastDevice != null ? p.lastDevice : "";
        update.lastBrowser = p.lastBrowser != null ? p.lastBrowser : "";
        update.topPages = topK(p.pageCounts, 5);
        update.topFeatures = topK(p.featureCounts, 3);
        update.action = action;
        update.timestamp = formatTimestamp(System.currentTimeMillis());
        update.trigger = trigger;
        return update;
    }

    private List<String> computeChangedFields(ProfileUpdate before, ProfileUpdate after) {
        if (before == null) {
            return new ArrayList<>(List.of("canonical_id", "tenant_id", "total_events", "first_seen", "last_seen"));
        }
        List<String> changed = new ArrayList<>();
        if (before.totalEvents != after.totalEvents) changed.add("total_events");
        if (before.totalSessions != after.totalSessions) changed.add("total_sessions");
        if (before.lastSeen != after.lastSeen) changed.add("last_seen");
        if (before.pageViews != after.pageViews) changed.add("page_views");
        if (before.clicks != after.clicks) changed.add("clicks");
        if (before.logins != after.logins) changed.add("logins");
        if (before.featureUses != after.featureUses) changed.add("feature_uses");
        if (!Objects.equals(before.lastPage, after.lastPage)) changed.add("last_page");
        if (!Objects.equals(before.lastCountry, after.lastCountry)) changed.add("last_country");
        if (!Objects.equals(before.lastDevice, after.lastDevice)) changed.add("last_device");
        if (!Objects.equals(before.lastBrowser, after.lastBrowser)) changed.add("last_browser");
        if (before.avgSessionDurationSec != after.avgSessionDurationSec) changed.add("avg_session_duration_sec");
        return changed;
    }

    private String formatTimestamp(long epochMs) {
        return Instant.ofEpochMilli(epochMs)
                .atOffset(ZoneOffset.UTC)
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
    }

    private long sumBuckets(Map<String, Long> buckets, LocalDate referenceDate, int days) {
        long sum = 0;
        for (int i = 0; i < days; i++) {
            String key = referenceDate.minusDays(i).format(DateTimeFormatter.ISO_LOCAL_DATE);
            sum += buckets.getOrDefault(key, 0L);
        }
        return sum;
    }

    private void pruneBuckets(UserProfileState profile) {
        LocalDate cutoff = LocalDate.now(ZoneOffset.UTC).minusDays(BUCKET_RETENTION_DAYS);
        profile.dailyBuckets.entrySet().removeIf(e -> {
            try {
                return LocalDate.parse(e.getKey()).isBefore(cutoff);
            } catch (Exception ex) {
                return true;
            }
        });
        profile.dailySessions.entrySet().removeIf(e -> {
            try {
                return LocalDate.parse(e.getKey()).isBefore(cutoff);
            } catch (Exception ex) {
                return true;
            }
        });
    }

    private long sumSessions(Map<String, java.util.Set<String>> dailySessions, LocalDate referenceDate, int days) {
        java.util.Set<String> allSessions = new java.util.HashSet<>();
        for (int i = 0; i < days; i++) {
            String key = referenceDate.minusDays(i).format(DateTimeFormatter.ISO_LOCAL_DATE);
            java.util.Set<String> daySessions = dailySessions.get(key);
            if (daySessions != null) {
                allSessions.addAll(daySessions);
            }
        }
        return allSessions.size();
    }

    private List<String> topK(Map<String, Long> counts, int k) {
        return counts.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .limit(k)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
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
}
