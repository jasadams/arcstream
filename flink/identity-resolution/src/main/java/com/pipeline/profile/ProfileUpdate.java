package com.pipeline.profile;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.List;

public class ProfileUpdate implements Serializable {
    @JsonProperty("canonical_id") public String canonicalId;
    @JsonProperty("tenant_id") public String tenantId;
    @JsonProperty("user_id") public String userId;
    @JsonProperty("first_seen") public long firstSeen;
    @JsonProperty("last_seen") public long lastSeen;
    @JsonProperty("updated_at") public long updatedAt;
    @JsonProperty("total_events") public long totalEvents;
    @JsonProperty("total_sessions") public long totalSessions;
    @JsonProperty("events_1d") public long events1d;
    @JsonProperty("events_7d") public long events7d;
    @JsonProperty("events_30d") public long events30d;
    @JsonProperty("events_90d") public long events90d;
    @JsonProperty("sessions_1d") public long sessions1d;
    @JsonProperty("sessions_7d") public long sessions7d;
    @JsonProperty("sessions_30d") public long sessions30d;
    @JsonProperty("sessions_90d") public long sessions90d;
    @JsonProperty("avg_session_duration_sec") public long avgSessionDurationSec;
    @JsonProperty("current_session_active") public boolean currentSessionActive;
    @JsonProperty("current_session_duration_sec") public long currentSessionDurationSec;
    @JsonProperty("page_views") public long pageViews;
    @JsonProperty("clicks") public long clicks;
    @JsonProperty("logins") public long logins;
    @JsonProperty("feature_uses") public long featureUses;
    @JsonProperty("last_page") public String lastPage;
    @JsonProperty("last_country") public String lastCountry;
    @JsonProperty("last_device") public String lastDevice;
    @JsonProperty("last_browser") public String lastBrowser;
    @JsonProperty("top_pages") public List<String> topPages;
    @JsonProperty("top_features") public List<String> topFeatures;
    @JsonProperty("action") public String action;
    @JsonProperty("changed_fields") public List<String> changedFields;
    @JsonProperty("timestamp") public String timestamp;
    @JsonProperty("trigger") public String trigger;
}
