/*
[dependencies]
serde = { version = "1", features = ["derive"] }
serde_json = "1"
chrono = { version = "0.4", features = ["serde"] }
*/

// Arcstream application policy only. Durable ownership, keyed lookup, MERGE and
// checkpoints belong to Streamr; this scalar function has no external state.
use arroyo_udf_plugin::udf;
use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Default, Serialize, Deserialize)]
struct Profile {
    canonical_id: String,
    tenant_id: String,
    user_id: String,
    first_seen: i64,
    last_seen: i64,
    updated_at: i64,
    total_events: i64,
    total_sessions: i64,
    events_1d: i64,
    events_7d: i64,
    events_30d: i64,
    events_90d: i64,
    sessions_1d: i64,
    sessions_7d: i64,
    sessions_30d: i64,
    sessions_90d: i64,
    avg_session_duration_sec: i64,
    current_session_active: bool,
    current_session_duration_sec: i64,
    page_views: i64,
    clicks: i64,
    logins: i64,
    feature_uses: i64,
    last_page: String,
    last_country: String,
    last_device: String,
    last_browser: String,
    top_pages: Vec<String>,
    top_features: Vec<String>,
    action: String,
    changed_fields: Vec<String>,
    timestamp: String,
    trigger: String,
}

#[derive(Default, Serialize, Deserialize)]
struct State {
    replay_mode: Option<bool>,
    profile: Profile,
    daily_events: BTreeMap<NaiveDate, i64>,
    daily_sessions: BTreeMap<NaiveDate, i64>,
    pages: BTreeMap<String, i64>,
    features: BTreeMap<String, i64>,
    session_id: Option<String>,
    session_start: i64,
    session_deadline: i64,
    closed_sessions: i64,
    closed_duration_ms: i64,
    activity_id: String,
    decay_live: bool,
    decayed: [bool; 3],
    previous_emitted: Option<Profile>,
    outputs: Vec<Envelope>,
    timer_seeds: Vec<TimerSeed>,
}

#[derive(Serialize, Deserialize)]
struct TimerSeed {
    tenant_id: String,
    canonical_id: String,
    generation: String,
    kind: String,
    deadline_time: String,
    progress_time: String,
    observed_ms: Option<i64>,
    replay_mode: bool,
}

#[derive(Serialize, Deserialize)]
struct Envelope {
    before: Option<Profile>,
    after: Profile,
    op: String,
}

#[derive(Deserialize)]
struct Signal {
    kind: String,
    tenant_id: String,
    canonical_id: String,
    // Native SESSION controls carry the last source event's identity, so old
    // closures cannot reset a subsequent active session or emit stale decay.
    generation: String,
    logical_time: String,
    #[serde(default)]
    event_id: String,
    #[serde(default)]
    event_type: String,
    #[serde(default)]
    user_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    page_url: Option<String>,
    #[serde(default)]
    country: Option<String>,
    #[serde(default)]
    device_type: Option<String>,
    #[serde(default)]
    browser: Option<String>,
    #[serde(default)]
    feature_name: Option<String>,
    // Optional fixture/instrumentation observation clock. Business values never
    // read it. Normal runtime uses the current invocation's wall clock instead.
    observed_ms: Option<i64>,
    #[serde(default)]
    replay_mode: bool,
    effective_ms: Option<i64>,
    source_ms: Option<i64>,
    #[serde(default)]
    freeze_observation: bool,
}

const DAY_MS: i64 = 86_400_000;
const SESSION_MS: i64 = 1_800_000;
const DECAY_DAYS: [i64; 3] = [1, 7, 30];

fn epoch_ms(value: &str) -> i64 {
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(value) {
        return timestamp.timestamp_millis();
    }
    NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f"))
        .expect("Arcstream profile signal requires a valid UTC event timestamp")
        .and_utc().timestamp_millis()
}

fn date(ms: i64) -> NaiveDate {
    DateTime::<Utc>::from_timestamp_millis(ms)
        .expect("Arcstream profile timestamp outside supported calendar").date_naive()
}

fn text_time(ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(ms)
        .expect("Arcstream profile timestamp outside supported calendar")
        .format("%Y-%m-%d %H:%M:%S%.3f").to_string()
}

fn sum_days(buckets: &BTreeMap<NaiveDate, i64>, reference: NaiveDate, days: i64) -> i64 {
    let start = reference - Duration::days(days - 1);
    buckets.range(start..=reference).map(|(_, count)| count).sum()
}

fn top(counts: &BTreeMap<String, i64>, limit: usize) -> Vec<String> {
    let mut items: Vec<_> = counts.iter().collect();
    items.sort_by(|(a, x), (b, y)| y.cmp(x).then_with(|| a.cmp(b)));
    items.into_iter().take(limit).map(|(item, _)| item.clone()).collect()
}

fn nonempty(value: &Option<String>) -> Option<&str> {
    value.as_deref().filter(|value| !value.is_empty())
}

fn emit(state: &mut State, logical_ms: i64, observed_ms: i64, trigger: &str) {
    let reference = date(logical_ms);
    let p = &mut state.profile;
    p.events_1d = sum_days(&state.daily_events, reference, 1);
    p.events_7d = sum_days(&state.daily_events, reference, 7);
    p.events_30d = sum_days(&state.daily_events, reference, 30);
    p.events_90d = sum_days(&state.daily_events, reference, 90);
    p.sessions_1d = sum_days(&state.daily_sessions, reference, 1);
    p.sessions_7d = sum_days(&state.daily_sessions, reference, 7);
    p.sessions_30d = sum_days(&state.daily_sessions, reference, 30);
    p.sessions_90d = sum_days(&state.daily_sessions, reference, 90);
    p.avg_session_duration_sec = if state.closed_sessions > 0 {
        state.closed_duration_ms / state.closed_sessions / 1000
    } else { 0 };
    p.current_session_active = state.session_id.is_some();
    p.current_session_duration_sec = if p.current_session_active && state.replay_mode == Some(true) {
        (logical_ms - state.session_start).max(0) / 1000
    } else if p.current_session_active && state.session_start > 0 {
        (observed_ms - state.session_start) / 1000
    } else { 0 };
    p.top_pages = top(&state.pages, 5);
    p.top_features = top(&state.features, 3);
    p.updated_at = observed_ms;
    p.timestamp = text_time(observed_ms);
    p.trigger = trigger.to_string();
    p.action = if state.previous_emitted.is_none() { "create" } else { "update" }.to_string();
    let mut changed = Vec::new();
    if let Some(old) = &state.previous_emitted {
        macro_rules! changed {
            ($field:ident) => { if old.$field != p.$field { changed.push(stringify!($field).to_string()); } };
        }
        changed!(total_events);
        changed!(total_sessions);
        changed!(last_seen);
        changed!(page_views);
        changed!(clicks);
        changed!(logins);
        changed!(feature_uses);
        changed!(last_page);
        changed!(last_country);
        changed!(last_device);
        changed!(last_browser);
        changed!(avg_session_duration_sec);
    } else {
        changed = ["canonical_id", "tenant_id", "total_events", "first_seen", "last_seen"]
            .iter().map(|s| s.to_string()).collect();
    }
    p.changed_fields = changed;
    let after = p.clone();
    state.outputs.push(Envelope {
        before: state.previous_emitted.clone(),
        after: after.clone(),
        op: if state.previous_emitted.is_none() { "c" } else { "u" }.to_string(),
    });
    state.previous_emitted = Some(after);
}

// Native controls can arrive in a different order across the four timer
// branches. Catch up due deadlines in timestamp order, never from wall clock.
// Fresh input cancels old timers using the generation guard before this runs.
fn advance(state: &mut State, until_ms: i64, inclusive: bool, observed_ms: i64) {
    loop {
        let session_due = state.session_id.as_ref().map(|_| state.session_deadline);
        let decay_due = if state.decay_live {
            DECAY_DAYS.iter().enumerate().filter(|(i, _)| !state.decayed[*i])
                .map(|(i, days)| (state.profile.last_seen + days * DAY_MS, i)).min()
        } else { None };
        let next = session_due.into_iter().map(|due| (due, None))
            .chain(decay_due.into_iter().map(|(due, i)| (due, Some(i)))).min_by_key(|(due, _)| *due);
        let Some((deadline, decay_index)) = next else { break; };
        if deadline > until_ms || (deadline == until_ms && !inclusive) { break; }
        if let Some(i) = decay_index {
            state.decayed[i] = true;
            emit(state, deadline, observed_ms, ["window_decay_1d", "window_decay_7d", "window_decay_30d"][i]);
        } else {
            state.closed_sessions += 1;
            state.closed_duration_ms += (state.profile.last_seen - state.session_start).max(0);
            state.session_id = None;
            state.session_start = 0;
            state.session_deadline = 0;
            state.decay_live = true;
            emit(state, deadline, observed_ms, "session_timeout");
        }
    }
}

#[udf]
fn arc_profile_transition(previous: &str, signal: &str, timer_watermark_ms: i64, previous_watermark_ms: i64) -> String {
    let signal: Signal = serde_json::from_str(signal).expect("invalid Arcstream profile signal schema");
    assert!(!signal.tenant_id.is_empty() && !signal.canonical_id.is_empty(), "profile identity is required");
    let mut state: State = if previous == "{}" {
        State::default()
    } else { serde_json::from_str(previous).expect("invalid persisted Arcstream profile state") };
    state.outputs.clear();
    state.timer_seeds.clear();
    if state.previous_emitted.is_some() {
        assert!(state.profile.tenant_id == signal.tenant_id && state.profile.canonical_id == signal.canonical_id,
                "native profile state ownership mismatch");
    }
    let raw_ms = epoch_ms(&signal.logical_time);
    let observed_ms = signal.observed_ms.unwrap_or_else(|| Utc::now().timestamp_millis());
    let logical_ms = if signal.kind == "event" && !signal.replay_mode &&
        (raw_ms > observed_ms + 60_000 || raw_ms < observed_ms - 91 * DAY_MS) {
        observed_ms
    } else { raw_ms };
    if signal.kind == "event" {
        assert!(signal.effective_ms == Some(logical_ms), "profile normalization effective_ms differs from selected clock policy");
        let source_ms = if !signal.replay_mode && raw_ms > observed_ms + 60_000 { observed_ms } else { raw_ms };
        assert!(signal.source_ms == Some(source_ms), "profile source progress differs from original future-only source clamp policy");
    }
    if let Some(mode) = state.replay_mode {
        assert!(mode == signal.replay_mode, "profile clock mode cannot change within persisted state");
    }
    match signal.kind.as_str() {
        "event" => {
            assert!(!signal.event_id.is_empty() && signal.generation == signal.event_id, "event generation must be event_id");
            assert!(!signal.event_type.is_empty(), "event_type is required");
            if state.previous_emitted.is_none() {
                state.replay_mode = Some(signal.replay_mode);
                state.profile.tenant_id = signal.tenant_id;
                state.profile.canonical_id = signal.canonical_id;
                state.profile.first_seen = logical_ms;
                state.profile.last_seen = logical_ms;
            } else {
                // A prior source event may have advanced declared completeness
                // before this event overtakes asynchronous timer feedback.
                // Catch up ONLY prior progress: the current record is processed
                // before its own newly advanced watermark, as in Flink.
                advance(&mut state, previous_watermark_ms, true, observed_ms);
            }
            let p = &mut state.profile;
            p.last_seen = p.last_seen.max(logical_ms);
            p.total_events += 1;
            if let Some(user) = nonempty(&signal.user_id) { p.user_id = user.to_string(); }
            match signal.event_type.as_str() {
                "page_view" => p.page_views += 1,
                "click" => p.clicks += 1,
                "login" => p.logins += 1,
                "feature_used" => p.feature_uses += 1,
                _ => {},
            }
            *state.daily_events.entry(date(logical_ms)).or_default() += 1;
            if let Some(session) = nonempty(&signal.session_id) {
                if state.session_id.as_deref() != Some(session) {
                    p.total_sessions += 1;
                    *state.daily_sessions.entry(date(logical_ms)).or_default() += 1;
                    state.session_id = Some(session.to_string());
                    state.session_start = logical_ms;
                }
            }
            if let Some(page) = nonempty(&signal.page_url) {
                p.last_page = page.to_string();
                *state.pages.entry(page.to_string()).or_default() += 1;
            }
            if let Some(country) = nonempty(&signal.country) { p.last_country = country.to_string(); }
            if let Some(device) = nonempty(&signal.device_type) { p.last_device = device.to_string(); }
            if let Some(browser) = nonempty(&signal.browser) { p.last_browser = browser.to_string(); }
            if let Some(feature) = nonempty(&signal.feature_name) {
                *state.features.entry(feature.to_string()).or_default() += 1;
            }
            state.activity_id = signal.event_id;
            // Preserve the Java profile distinction: timer from the incoming
            // event, last_seen as MAX. A late event can move the timer earlier.
            state.session_deadline = logical_ms + SESSION_MS;
            state.decay_live = false;
            state.decayed = [false; 3];
            if !signal.replay_mode {
                let cutoff = date(observed_ms) - Duration::days(91);
                state.daily_events.retain(|day, _| *day >= cutoff);
                state.daily_sessions.retain(|day, _| *day >= cutoff);
            }
            // Existing-profile debounce uses MAX(last_seen), even when this
            // arriving record moved its session deadline backwards.
            let reference_ms = state.profile.last_seen;
            emit(&mut state, reference_ms, observed_ms, "event");
            for (kind, deadline_ms) in [("session_timeout", state.session_deadline),
                ("window_decay_1d", state.profile.last_seen + DAY_MS),
                ("window_decay_7d", state.profile.last_seen + 7 * DAY_MS),
                ("window_decay_30d", state.profile.last_seen + 30 * DAY_MS)] {
                state.timer_seeds.push(TimerSeed {
                    tenant_id: state.profile.tenant_id.clone(),
                    canonical_id: state.profile.canonical_id.clone(),
                    generation: state.activity_id.clone(),
                    kind: kind.to_string(), deadline_time: text_time(deadline_ms),
                    progress_time: text_time(timer_watermark_ms),
                    observed_ms: if signal.freeze_observation { signal.observed_ms } else { None },
                    replay_mode: signal.replay_mode,
                });
            }
        }
        "clock" => {},
        "session_timeout" | "window_decay_1d" | "window_decay_7d" | "window_decay_30d" => {
            if state.previous_emitted.is_some() && signal.generation == state.activity_id {
                advance(&mut state, logical_ms, true, observed_ms);
            }
        }
        _ => panic!("unknown Arcstream profile signal kind"),
    }
    serde_json::to_string(&state).expect("Arcstream profile state serialization failed")
}
