package com.pipeline.identity;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

public class MergeEvent implements Serializable {
    @JsonProperty("old_canonical_id") public String oldCanonicalId;
    @JsonProperty("canonical_id") public String canonicalId;
    @JsonProperty("tenant_id") public String tenantId;
    @JsonProperty("merged_at") public String mergedAt;

    public static MergeEvent of(String oldCanonicalId, String newCanonicalId, String tenantId, String mergedAt) {
        MergeEvent e = new MergeEvent();
        e.oldCanonicalId = oldCanonicalId;
        e.canonicalId = newCanonicalId;
        e.tenantId = tenantId;
        e.mergedAt = mergedAt;
        return e;
    }
}
