package com.pipeline.profile;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class UserProfileState implements Serializable {
    public String canonicalId;
    public String userId = "";
    public String tenantId;

    public long firstSeenEpochMs;
    public long lastSeenEpochMs;
    public long totalEvents;

    public long pageViews;
    public long clicks;
    public long signups;
    public long logins;
    public long featureUses;

    // Sessions are sequential per user, so counting transitions to a new
    // session_id equals counting distinct sessions — without retaining every
    // session UUID forever (the old Set<String> grew unboundedly and was
    // re-serialized into RocksDB on every event).
    public long sessionsStarted;
    public String currentSessionId;
    public long currentSessionStartMs;
    public long closedSessionCount;
    public long totalSessionDurationMs;

    // "2026-05-23" -> event count for that day
    public Map<String, Long> dailyBuckets = new HashMap<>();
    // "2026-05-23" -> sessions STARTED that day. Attributing a session to
    // exactly one day makes window sums exact distinct counts.
    public Map<String, Long> dailySessionStarts = new HashMap<>();

    public String lastPage = "";
    public String lastCountry = "";
    public String lastDevice = "";
    public String lastBrowser = "";

    // page_url -> count
    public Map<String, Long> pageCounts = new HashMap<>();
    // feature_name -> count
    public Map<String, Long> featureCounts = new HashMap<>();

    // Scheduled timer timestamps (0 = not scheduled)
    public long sessionTimer;
    public long decayTimer1d;
    public long decayTimer7d;
    public long decayTimer30d;

    // Pending debounced emission (processing-time timer, 0 = none)
    public long emitTimer;

    // Snapshot of the comparable fields from the last EMITTED ProfileUpdate,
    // so changedFields stays meaningful across debounced emissions.
    public EmittedSnapshot lastEmitted;

    public static class EmittedSnapshot implements Serializable {
        public long totalEvents;
        public long totalSessions;
        public long lastSeen;
        public long pageViews;
        public long clicks;
        public long logins;
        public long featureUses;
        public long avgSessionDurationSec;
        public String lastPage = "";
        public String lastCountry = "";
        public String lastDevice = "";
        public String lastBrowser = "";
    }
}
