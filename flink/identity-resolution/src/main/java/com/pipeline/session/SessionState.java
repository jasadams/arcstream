package com.pipeline.session;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SessionState implements Serializable {
    public String sessionId;
    public String canonicalId;
    public String tenantId;
    public long startTimeMs;
    public long lastEventTimeMs;
    public long registeredTimerMs;
    public int eventCount;
    public Set<String> pages = new HashSet<>();
    public Map<String, Integer> eventTypeCounts = new HashMap<>();
    public String deviceType = "";
    public String browser = "";
    public String country = "";
}
