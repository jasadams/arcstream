/*
[dependencies]
serde_json = "1"
chrono = "0.4"
*/
use arroyo_udf_plugin::udf;
use chrono::{DateTime, NaiveDateTime, Utc};

// Arcstream carries its explicit source event_time through this stateless adapter.
// Native progress belongs to the downstream owner; normalization never drops late arrivals.
#[udf]
fn arc_profile_signal(raw: &str, event_timestamp: &str, replay_mode: bool) -> String {
    let mut event: serde_json::Value =
        serde_json::from_str(raw).expect("invalid unified event JSON");
    let event = event
        .as_object_mut()
        .expect("unified event must be an object");
    for key in ["event_id", "event_type", "tenant_id", "canonical_id"] {
        assert!(
            event
                .get(key)
                .and_then(|value| value.as_str())
                .is_some_and(|value| !value.is_empty()),
            "unified event requires nonempty identity/type fields"
        );
    }
    let generation = event["event_id"].clone();
    let is_clock = ["event_type", "tenant_id", "canonical_id"]
        .iter()
        .all(|key| event.get(*key).and_then(|value| value.as_str()) == Some("__arc_clock__"));
    event.insert(
        "kind".into(),
        serde_json::json!(if is_clock { "clock" } else { "event" }),
    );
    event.insert("generation".into(), generation);
    event.insert("logical_time".into(), serde_json::json!(event_timestamp));
    event.insert("replay_mode".into(), serde_json::json!(replay_mode));
    let source_ms = DateTime::parse_from_rfc3339(event_timestamp)
        .map(|value| value.timestamp_millis())
        .or_else(|_| {
            NaiveDateTime::parse_from_str(event_timestamp, "%Y-%m-%d %H:%M:%S%.f")
                .map(|value| value.and_utc().timestamp_millis())
        })
        .or_else(|_| {
            NaiveDateTime::parse_from_str(event_timestamp, "%Y-%m-%dT%H:%M:%S%.f")
                .map(|value| value.and_utc().timestamp_millis())
        })
        .expect("invalid source event_time");
    event.insert("session_source_ms".into(), serde_json::json!(source_ms));
    let observed_ms = Utc::now().timestamp_millis();
    let effective_ms = if !replay_mode
        && (source_ms > observed_ms + 60_000 || source_ms < observed_ms - 91 * 86_400_000)
    {
        observed_ms
    } else {
        source_ms
    };
    event.insert("observed_ms".into(), serde_json::json!(observed_ms));
    event.insert("effective_ms".into(), serde_json::json!(effective_ms));
    // Match Arcstream's source timestamp assigner independently from its profile
    // value clamp: only far-future live input changes watermark assignment.
    let assigned_ms = if !replay_mode && source_ms > observed_ms + 60_000 {
        observed_ms
    } else {
        source_ms
    };
    event.insert("source_ms".into(), serde_json::json!(assigned_ms));
    if is_clock {
        event.insert("deadline_time".into(), serde_json::json!(event_timestamp));
        let progress = DateTime::<Utc>::from_timestamp_millis(assigned_ms - 5001)
            .expect("clock marker outside supported timestamp range")
            .format("%Y-%m-%d %H:%M:%S%.3f")
            .to_string();
        event.insert("progress_time".into(), serde_json::json!(progress));
    }
    serde_json::to_string(event).expect("cannot serialize Arcstream profile signal")
}
