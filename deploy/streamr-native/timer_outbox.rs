/*
[dependencies]
serde_json = "1"
chrono = "0.4"
*/
use arroyo_udf_plugin::udf;
use chrono::{DateTime, Utc};

// Technical transport only: business state, deadlines and declared progress stay
// unchanged. Native source admission and emission use separate clocks.
#[udf]
fn arc_timer_outbox(payload: &str, dispatch_ms: i64, progress_ms: i64) -> String {
    fn text_time(value: i64) -> String {
        let time = DateTime::<Utc>::from_timestamp_millis(value)
            .expect("Arcstream technical timer timestamp outside chrono range");
        assert!(time.timestamp_nanos_opt().is_some(),
            "Arcstream technical timer dispatch exhausted native nanosecond timestamp range");
        time.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
    }
    assert!(dispatch_ms > progress_ms, "technical dispatch must exceed declared progress");
    let mut frame: serde_json::Value = serde_json::from_str(payload)
        .expect("invalid Arcstream scalar outbox payload");
    frame.as_object_mut().expect("Arcstream scalar outbox must be an object").insert(
        "technical_clock".into(), serde_json::json!({
            "tenant_id":"", "canonical_id":"", "session_id":"",
            "generation":"arc-technical-clock", "kind":"clock",
            "deadline_time":text_time(progress_ms), "progress_time":text_time(progress_ms),
            "dispatch_time":text_time(dispatch_ms), "observed_ms":null, "replay_mode":false
        }));
    serde_json::to_string(&frame).expect("cannot serialize Arcstream timer outbox")
}
