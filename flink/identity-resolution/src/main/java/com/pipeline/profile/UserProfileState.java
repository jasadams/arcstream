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

    public java.util.Set<String> allSessionIds = new java.util.HashSet<>();
    public String currentSessionId;
    public long currentSessionStartMs;
    public long closedSessionCount;
    public long totalSessionDurationMs;

    // "2026-05-23" -> event count for that day
    public Map<String, Long> dailyBuckets = new HashMap<>();
    // "2026-05-23" -> set of session_ids seen that day (stored as count of unique sessions)
    public Map<String, java.util.Set<String>> dailySessions = new HashMap<>();

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
}
