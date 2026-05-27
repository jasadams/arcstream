package com.pipeline.identity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RawEvent implements Serializable {
    @JsonProperty("event_id") public String eventId;
    @JsonProperty("event_type") public String eventType;
    @JsonProperty("tenant_id") public String tenantId;
    @JsonProperty("event_time") public String eventTime;
    @JsonProperty("anonymous_id") public String anonymousId;
    @JsonProperty("user_id") public String userId;
    @JsonProperty("session_id") public String sessionId;
    @JsonProperty("page_url") public String pageUrl;
    @JsonProperty("referrer") public String referrer;
    @JsonProperty("element_id") public String elementId;
    @JsonProperty("feature_name") public String featureName;
    @JsonProperty("device_type") public String deviceType;
    @JsonProperty("browser") public String browser;
    @JsonProperty("os") public String os;
    @JsonProperty("country") public String country;
    @JsonProperty("properties") public String properties;

    public boolean hasUserId() {
        return userId != null && !userId.isEmpty();
    }
}
