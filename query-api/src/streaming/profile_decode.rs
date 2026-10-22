//! Adapt the native SQL changelog to the application's profile subscription.
//! A retraction is the before-image of one update, not a separate zero profile.
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
];

fn full_profile(mut value: Value, action: &str) -> Result<FlatProfileUpdate, String> {
    let fields = value
        .as_object()
        .ok_or("native profile image must be an object")?;
    if fields.len() != PROFILE_FIELDS.len()
        || PROFILE_FIELDS.iter().any(|key| !fields.contains_key(*key))
    {
        return Err("native profile must contain all 21 release business fields".into());
    }
    // Action is transport metadata derived from the native CDC operation.
    value
        .as_object_mut()
        .unwrap()
        .insert("action".into(), Value::String(action.into()));
    let profile: FlatProfileUpdate = serde_json::from_value(value).map_err(|e| e.to_string())?;
    if profile.tenant_id.is_empty() || profile.canonical_id.is_empty() {
        return Err("native profile requires nonempty tenant and canonical identity".into());
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
    let op = value
        .get("op")
        .and_then(Value::as_str)
        .ok_or("native CDC op must be text")?;
    let before = value.get("before").ok_or("native CDC requires before")?;
    let after = value.get("after").ok_or("native CDC requires after")?;
    match op {
        "c" | "r" if before.is_null() && !after.is_null() => {
            Ok(Some(full_profile(after.clone(), "create")?.into_message()))
        }
        "u" if !before.is_null() && !after.is_null() => {
            let previous = full_profile(before.clone(), "update")?;
            let current = full_profile(after.clone(), "update")?;
            if previous.tenant_id != current.tenant_id
                || previous.canonical_id != current.canonical_id
            {
                return Err("native CDC update cannot change the profile identity".into());
            }
            Ok(Some(current.into_message()))
        }
        // Profile lifecycle closes/decays by updating its durable row. A physical
        // deletion requires an explicit application deletion contract.
        "d" => Err("native profile deletion is not an application lifecycle update".into()),
        _ => Err("invalid native profile CDC operation or image combination".into()),
    }
}
