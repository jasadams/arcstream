//! Validate native MERGE RETURNING profile snapshots for application subscriptions.
//! Business values and create/update action are supplied by the native SQL producer.
use serde_json::Value;

use super::types::{FlatProfileUpdate, ProfileUpdateMessage};

const PROFILE_FIELDS: &[&str] = &[
    "canonical_id",
    "tenant_id",
    "user_id",
    "first_seen",
    "last_seen",
    "total_events",
    "events_1d",
    "events_7d",
    "events_30d",
    "events_90d",
    "avg_session_duration_sec",
    "page_views",
    "clicks",
    "logins",
    "feature_uses",
    "last_page",
    "last_country",
    "last_device",
    "last_browser",
    "top_pages",
    "top_features",
    "action",
];

fn full_profile(value: Value) -> Result<FlatProfileUpdate, String> {
    let fields = value
        .as_object()
        .ok_or("native profile image must be an object")?;
    if fields.len() != PROFILE_FIELDS.len()
        || PROFILE_FIELDS.iter().any(|key| !fields.contains_key(*key))
    {
        return Err("native profile must contain all 22 release fields".into());
    }
    let profile: FlatProfileUpdate = serde_json::from_value(value).map_err(|e| e.to_string())?;
    if profile.tenant_id.is_empty() || profile.canonical_id.is_empty() {
        return Err("native profile requires nonempty tenant and canonical identity".into());
    }
    if !matches!(profile.action.as_str(), "create" | "update") {
        return Err("native profile action must be create or update".into());
    }
    for timestamp in [profile.first_seen, profile.last_seen] {
        if chrono::DateTime::from_timestamp_millis(timestamp).is_none() {
            return Err("profile timestamp is outside the application timestamp range".into());
        }
    }
    if profile
        .avg_session_duration_sec
        .is_some_and(|seconds| !seconds.is_finite() || seconds < 0.0)
    {
        return Err("completed-session average must be null or finite nonnegative seconds".into());
    }
    Ok(profile)
}

pub fn decode(payload: &[u8]) -> Result<Option<ProfileUpdateMessage>, String> {
    let value: Value = serde_json::from_slice(payload).map_err(|e| e.to_string())?;
    // Kafka tombstones contain no profile to publish.
    if value.is_null() {
        return Ok(None);
    }
    full_profile(value).map(|profile| Some(profile.into_message()))
}
