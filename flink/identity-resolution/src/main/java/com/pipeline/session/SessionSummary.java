package com.pipeline.session;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class SessionSummary implements Serializable {
    @JsonProperty("session_id") public String sessionId;
    @JsonProperty("canonical_id") public String canonicalId;
    @JsonProperty("tenant_id") public String tenantId;
    @JsonProperty("start_time") public String startTime;
    @JsonProperty("end_time") public String endTime;
    @JsonProperty("duration_sec") public long durationSec;
    @JsonProperty("event_count") public int eventCount;
    @JsonProperty("pages") public List<String> pages;
    @JsonProperty("event_types") public Map<String, Integer> eventTypes;
    @JsonProperty("device_type") public String deviceType;
    @JsonProperty("browser") public String browser;
    @JsonProperty("country") public String country;
}
