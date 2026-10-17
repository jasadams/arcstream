/*
[dependencies]
serde = { version = "1", features = ["derive"] }
serde_json = "1"
chrono = "0.4"
*/
// Arcstream's session lifecycle is application policy. This function has no
// external state; Streamr owns keyed lookup, mutations and checkpoint recovery.
use arroyo_udf_plugin::udf;
use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

const TIMEOUT_MS: i64 = 1_800_000;

#[derive(Clone, Serialize, Deserialize)]
struct Active {
    tenant_id: String,
    session_id: String,
    canonical_id: String,
    start_ms: i64,
    last_ms: i64,
    event_count: i64,
    pages: BTreeSet<String>,
    event_types: BTreeMap<String, i64>,
    device_type: String,
    browser: String,
    country: String,
    generation: String,
}
#[derive(Serialize, Deserialize)]
struct Summary {
    session_id: String,
    canonical_id: String,
    tenant_id: String,
    start_time: String,
    end_time: String,
    duration_sec: i64,
    event_count: i64,
    pages: Vec<String>,
    event_types: BTreeMap<String, i64>,
    device_type: String,
    browser: String,
    country: String,
}
#[derive(Serialize, Deserialize)]
struct TimerSeed {
    tenant_id: String,
    session_id: String,
    generation: String,
    kind: String,
    deadline_time: String,
    progress_time: String,
}
#[derive(Default, Serialize, Deserialize)]
struct Decision {
    state: Option<Active>,
    mode: String,
    outputs: Vec<Summary>,
    timer_seeds: Vec<TimerSeed>,
}
#[derive(Deserialize)]
struct Signal {
    kind: String,
    tenant_id: String,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    canonical_id: String,
    generation: String,
    logical_time: String,
    #[serde(default)]
    event_time: Option<String>,
    #[serde(default)]
    session_source_ms: Option<i64>,
    #[serde(default)]
    event_id: String,
    #[serde(default)]
    event_type: String,
    #[serde(default)]
    page_url: Option<String>,
    #[serde(default)]
    device_type: Option<String>,
    #[serde(default)]
    browser: Option<String>,
    #[serde(default)]
    country: Option<String>,
}
fn epoch(value: &str) -> i64 {
    if let Ok(value) = DateTime::parse_from_rfc3339(value) {
        return value.timestamp_millis();
    }
    NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f"))
        .expect("Arcstream session requires a valid UTC source timestamp")
        .and_utc()
        .timestamp_millis()
}
fn text(ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(ms)
        .expect("Arcstream session timestamp outside supported calendar")
        .format("%Y-%m-%d %H:%M:%S%.3f")
        .to_string()
}
fn selected(value: &Option<String>) -> Option<&str> {
    value.as_deref().filter(|value| !value.is_empty())
}
fn summary(state: Active) -> Summary {
    Summary {
        session_id: state.session_id,
        canonical_id: state.canonical_id,
        tenant_id: state.tenant_id,
        start_time: text(state.start_ms),
        end_time: text(state.last_ms),
        duration_sec: (state.last_ms - state.start_ms) / 1000,
        event_count: state.event_count,
        pages: state.pages.into_iter().collect(),
        event_types: state.event_types,
        device_type: state.device_type,
        browser: state.browser,
        country: state.country,
    }
}

#[udf]
fn arc_session_transition(
    previous: &str,
    signal: &str,
    watermark_ms: i64,
    previous_watermark_ms: i64,
) -> String {
    let signal: Signal = serde_json::from_str(signal).expect("invalid Arcstream session signal");
    assert!(!signal.tenant_id.is_empty(), "session tenant is required");
    let session_id = signal.session_id.as_deref().unwrap_or("");
    let mut decision: Decision = if previous == "{}" {
        Decision::default()
    } else {
        serde_json::from_str(previous).expect("invalid persisted Arcstream session state")
    };
    decision.outputs.clear();
    decision.timer_seeds.clear();
    if let Some(state) = &decision.state {
        assert!(
            state.tenant_id == signal.tenant_id && state.session_id == session_id,
            "session state ownership differs"
        );
    }
    match signal.kind.as_str() {
        "event" => {
            assert!(
                !signal.event_id.is_empty() && signal.generation == signal.event_id,
                "session generation must be the input event_id"
            );
            assert!(
                !signal.event_type.is_empty(),
                "session event type is required"
            );
            // Session values and watermark assignment retain raw source time;
            // profile live-clamping never changes session business semantics.
            let event_ms = epoch(signal.event_time.as_deref().unwrap_or(&signal.logical_time));
            assert!(
                signal.session_source_ms == Some(event_ms),
                "session source metadata must retain raw event time"
            );
            if !session_id.is_empty() {
                if decision
                    .state
                    .as_ref()
                    .is_some_and(|state| previous_watermark_ms >= state.last_ms + TIMEOUT_MS)
                {
                    decision
                        .outputs
                        .push(summary(decision.state.take().unwrap()));
                }
                let state = decision.state.get_or_insert_with(|| Active {
                    tenant_id: signal.tenant_id.clone(),
                    session_id: session_id.to_string(),
                    canonical_id: signal.canonical_id.clone(),
                    start_ms: event_ms,
                    // Preserve SessionState Java long default0 before MAX,
                    // including its epoch-zero end boundary for pre-epoch input.
                    last_ms: 0,
                    event_count: 0,
                    pages: BTreeSet::new(),
                    event_types: BTreeMap::new(),
                    device_type: String::new(),
                    browser: String::new(),
                    country: String::new(),
                    generation: String::new(),
                });
                assert!(
                    !state.canonical_id.is_empty(),
                    "session canonical identity is required"
                );
                state.last_ms = state.last_ms.max(event_ms);
                state.event_count += 1;
                if let Some(page) = selected(&signal.page_url) {
                    state.pages.insert(page.to_string());
                }
                *state
                    .event_types
                    .entry(signal.event_type.clone())
                    .or_default() += 1;
                if let Some(device) = selected(&signal.device_type) {
                    state.device_type = device.to_string();
                }
                if let Some(browser) = selected(&signal.browser) {
                    state.browser = browser.to_string();
                }
                if let Some(country) = selected(&signal.country) {
                    state.country = country.to_string();
                }
                state.generation = signal.event_id.clone();
                decision.timer_seeds.push(TimerSeed {
                    tenant_id: state.tenant_id.clone(),
                    session_id: state.session_id.clone(),
                    generation: state.generation.clone(),
                    kind: "session_summary_timeout".to_string(),
                    deadline_time: text(state.last_ms + TIMEOUT_MS),
                    progress_time: text(watermark_ms),
                });
            } else {
                decision.timer_seeds.push(TimerSeed {
                    tenant_id: signal.tenant_id.clone(),
                    session_id: String::new(),
                    generation: signal.event_id.clone(),
                    kind: "clock".to_string(),
                    deadline_time: text(event_ms),
                    progress_time: text(watermark_ms),
                });
            }
        }
        "clock" => {
            decision.timer_seeds.push(TimerSeed {
                tenant_id: signal.tenant_id.clone(),
                session_id: session_id.to_string(),
                generation: signal.generation.clone(),
                kind: "clock".to_string(),
                deadline_time: text(epoch(&signal.logical_time)),
                progress_time: text(watermark_ms),
            });
        }
        "session_summary_timeout" => {
            if decision.state.as_ref().is_some_and(|state| {
                state.generation == signal.generation
                    && epoch(&signal.logical_time) >= state.last_ms + TIMEOUT_MS
            }) {
                decision
                    .outputs
                    .push(summary(decision.state.take().unwrap()));
            }
        }
        _ => panic!("unknown Arcstream session signal kind"),
    }
    decision.mode = if decision.state.is_some() {
        "keep"
    } else {
        "delete"
    }
    .to_string();
    serde_json::to_string(&decision).expect("Arcstream session decision serialization failed")
}
