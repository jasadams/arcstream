/*
[dependencies]
serde_json = "1"
*/

// ARC-3: Profile Updater UDF.
//
// Runtime-compiled Arroyo `#[udf]` that ports the read-modify-write core of
// `flink/identity-resolution/src/main/java/com/pipeline/profile/ProfileFunction.java`.
//
// The fork's SQL JSON functions are READ-ONLY, so the entire JSON-blob
// read-modify-write lives here. The SQL wrapper supplies the prior state blob
// (from `state_get`) plus the event fields, and we return the updated blob.
// The blob carries BOTH the internal accumulator state AND every emitted
// output field under its exact output key, so the SQL extraction is purely
// mechanical (`extract_json_string` + `CAST`).
//
// IMPORTANT: every emitted NUMERIC/TIMESTAMP field is stored as a decimal
// STRING (via `istr`), not a JSON number. The profile-updater SQL reads them
// with `extract_json_string(...)`, which returns NULL for a JSON *number* — so
// storing numbers as numbers silently nulled first_seen/last_seen/total_events/
// all counters in `profile-updates` (and thus the dashboard). `get_i64` parses
// these strings back so the internal read-modify-write is unaffected. This
// mirrors `current_session_active`, long stored as the string "true"/"false".
//
// Deviations from the ticket's illustrative signature, all deliberate:
//   * DROP `os`: no profile field tracks operating system, so it is unused.
//   * ADD `user_id`: the profile persists the last non-empty user_id and emits
//     it even on events whose user_id is empty (ProfileFunction.java lines
//     66-68 + 215). Without it the UDF could never populate user_id.
//
// Other intentional divergences from the Flink job (each marked inline):
//   * Sessions close on the NEXT session's first event, not via a 30-min
//     event-time timer. Arroyo SQL has no timer primitive here and the ticket
//     states the timer-close does not translate.
//   * `current_session_duration_sec` uses EVENT time (deterministic) rather
//     than wall-clock, unlike Java's System.currentTimeMillis().
//   * `updated_at` DOES use wall-clock millis (matches Java) because it is the
//     Pinot FULL-upsert comparison column and must be processing-time
//     monotonic so the most-recently-computed state always wins.
//   * `changed_fields` emits the full comparable field set every event
//     (explicitly acceptable per the spec); there is no cross-event "last
//     emitted" snapshot to diff against in this stateless-per-call UDF.

use arroyo_udf_plugin::udf;

use serde_json::{json, Map, Value};
use std::time::{SystemTime, UNIX_EPOCH};

const DAY_MS: i64 = 24 * 60 * 60 * 1000;
// Keep >= 90 days so every rolling window has full coverage; drop anything
// strictly older. Mirrors ProfileFunction.BUCKET_RETENTION_DAYS.
const BUCKET_RETENTION_DAYS: i64 = 91;
// Event-time sanity clamp, mirroring ProfileFunction.clampEventTime
// (MAX_PAST_MS / MAX_FUTURE_MS). A single malformed future-dated event
// (e.g. "2099-01-01...") would otherwise push prune_daily's reference day far
// into the future and wipe the entire bucket history; an ancient one would
// distort rolling windows. Clamp both extremes back to wall-clock now.
const MAX_PAST_MS: i64 = 91 * DAY_MS;
const MAX_FUTURE_MS: i64 = 60_000; // 60 s, matches Java MAX_FUTURE_MS

#[udf]
fn profile_step(
    blob: Option<&str>,
    event_type: &str,
    event_time: &str,
    user_id: Option<&str>,
    session_id: Option<&str>,
    page_url: Option<&str>,
    device_type: Option<&str>,
    browser: Option<&str>,
    country: Option<&str>,
    feature_name: Option<&str>,
) -> Option<String> {
    // Start from prior state, or a fresh default if there is none / it is
    // unparseable. ProfileFunction treats a null ValueState as a brand-new
    // profile.
    let mut state: Map<String, Value> = blob
        .and_then(|b| serde_json::from_str::<Value>(b).ok())
        .and_then(|v| match v {
            Value::Object(m) => Some(m),
            _ => None,
        })
        .unwrap_or_default();

    // --- Step 1: parse event_time -> epoch millis + date_key (YYYY-MM-DD) ---
    let (event_ms, date_key) = parse_event_time(event_time);
    let event_day = days_from_civil_str(&date_key);

    // --- Step 14 (precondition): detect new profile via first_seen == 0 ---
    let prev_first_seen = get_i64(&state, "first_seen");
    let is_new_profile = prev_first_seen == 0;

    // --- Step 2: first_seen / last_seen / total_events ---
    if is_new_profile {
        state.insert("first_seen".into(), istr(event_ms));
    }
    let prev_last_seen = get_i64(&state, "last_seen");
    let last_seen = prev_last_seen.max(event_ms);
    state.insert("last_seen".into(), istr(last_seen));
    let total_events = get_i64(&state, "total_events") + 1;
    state.insert("total_events".into(), istr(total_events));

    // --- Step 3: persist last non-empty user_id ---
    if let Some(uid) = non_empty(user_id) {
        state.insert("user_id".into(), json!(uid));
    }

    // --- Step 4: event-type counters (mirror Java switch). signups is an
    // internal accumulator that ProfileUpdate never emits. ---
    match event_type {
        "page_view" => bump_counter(&mut state, "page_views"),
        "click" => bump_counter(&mut state, "clicks"),
        "signup" => bump_counter(&mut state, "signups"),
        "login" => bump_counter(&mut state, "logins"),
        "feature_used" => bump_counter(&mut state, "feature_uses"),
        _ => {}
    }

    // --- Step 5: daily_buckets[date_key] += 1 ---
    bump_map(&mut state, "daily_buckets", &date_key);

    // --- Step 6: session transition. Close the prior open session FIRST (this
    // is "close only on next event arrival"), then start the new one. ---
    if let Some(sid) = non_empty(session_id) {
        let current_session_id = get_str(&state, "current_session_id");
        if current_session_id.as_deref() != Some(sid) {
            if current_session_id.is_some() {
                // Close previous session using the last_seen recorded BEFORE
                // this event (prev_last_seen), mirroring Java's session timer
                // close which uses lastSeen at close time.
                let start = get_i64(&state, "current_session_start_ms");
                let dur = (prev_last_seen - start).max(0);
                let closed = get_i64(&state, "closed_session_count") + 1;
                state.insert("closed_session_count".into(), json!(closed));
                let total_dur = get_i64(&state, "total_session_duration_ms") + dur;
                state.insert("total_session_duration_ms".into(), json!(total_dur));
            }
            let started = get_i64(&state, "sessions_started") + 1;
            state.insert("sessions_started".into(), json!(started));
            bump_map(&mut state, "daily_session_starts", &date_key);
            state.insert("current_session_id".into(), json!(sid));
            state.insert("current_session_start_ms".into(), json!(event_ms));
        }
    }

    // --- Step 8: prune daily maps older than retention. Reference day is the
    // later of this event's day and the newest bucket day, so out-of-order
    // backfill events never prune live data prematurely (Java prunes off
    // wall-clock; event-day is deterministic for reprocessing). ---
    prune_daily(&mut state, "daily_buckets", event_day);
    prune_daily(&mut state, "daily_session_starts", event_day);

    // --- Step 7: last_* fields + page/feature top-K counters ---
    if let Some(p) = non_empty(page_url) {
        state.insert("last_page".into(), json!(p));
        bump_map(&mut state, "page_counts", p);
    }
    if let Some(c) = non_empty(country) {
        state.insert("last_country".into(), json!(c));
    }
    if let Some(d) = non_empty(device_type) {
        state.insert("last_device".into(), json!(d));
    }
    if let Some(b) = non_empty(browser) {
        state.insert("last_browser".into(), json!(b));
    }
    if let Some(f) = non_empty(feature_name) {
        bump_map(&mut state, "feature_counts", f);
    }

    // --- Step 9: rolling window sums relative to THIS event's day. ---
    let (e1, e7, e30, e90) = window_sums(&state, "daily_buckets", event_day);
    let (s1, s7, s30, s90) = window_sums(&state, "daily_session_starts", event_day);

    // --- Step 10: avg session duration ---
    let closed = get_i64(&state, "closed_session_count");
    let avg_session_duration_sec = if closed > 0 {
        get_i64(&state, "total_session_duration_ms") / closed / 1000
    } else {
        0
    };

    // --- Step 11: current session active + duration (EVENT time) ---
    let cur_sid = get_str(&state, "current_session_id");
    let current_session_active = cur_sid.is_some();
    let current_session_duration_sec = match cur_sid {
        Some(_) => {
            let start = get_i64(&state, "current_session_start_ms");
            if start > 0 {
                (event_ms - start).max(0) / 1000
            } else {
                0
            }
        }
        None => 0,
    };

    // --- Step 12: top-K page/feature keys. Deterministic ordering
    // (count desc, then key asc) since HashMap iteration order is unspecified
    // in Java; this is a documented stabilization of Java's topK. ---
    let top_pages = top_k(&state, "page_counts", 5);
    let top_features = top_k(&state, "feature_counts", 3);

    // --- Step 13: updated_at = WALL-CLOCK now (Pinot FULL-upsert time col). ---
    let updated_at = wall_clock_ms();

    // --- Step 14: action create vs update ---
    let action = if is_new_profile { "create" } else { "update" };

    // --- Step 15: changed_fields (full comparable set) + trigger ---
    let changed_fields = json!([
        "total_events",
        "total_sessions",
        "last_seen",
        "page_views",
        "clicks",
        "logins",
        "feature_uses",
        "last_page",
        "last_country",
        "last_device",
        "last_browser",
        "avg_session_duration_sec"
    ])
    .to_string();

    // --- Step 16: write ALL emitted output fields into the blob under the
    // EXACT output keys so the SQL extraction is mechanical. ---
    let user_id_out = get_str(&state, "user_id").unwrap_or_default();
    state.insert("user_id".into(), json!(user_id_out));
    // All emitted numeric/timestamp fields go out as decimal STRINGS via `istr`
    // so the SQL's `extract_json_string(...) + CAST(... AS BIGINT)` reads them;
    // a JSON number would extract as NULL. (See `istr` doc.)
    state.insert("updated_at".into(), istr(updated_at));
    // total_sessions is the emitted alias for sessions_started.
    state.insert("total_sessions".into(), istr(get_i64(&state, "sessions_started")));
    state.insert("events_1d".into(), istr(e1));
    state.insert("events_7d".into(), istr(e7));
    state.insert("events_30d".into(), istr(e30));
    state.insert("events_90d".into(), istr(e90));
    state.insert("sessions_1d".into(), istr(s1));
    state.insert("sessions_7d".into(), istr(s7));
    state.insert("sessions_30d".into(), istr(s30));
    state.insert("sessions_90d".into(), istr(s90));
    state.insert("avg_session_duration_sec".into(), istr(avg_session_duration_sec));
    // Store as the literal string "true"/"false" so the SQL CAST(... AS BOOLEAN)
    // is unambiguous (extract_json_string yields the inner text).
    state.insert(
        "current_session_active".into(),
        json!(if current_session_active { "true" } else { "false" }),
    );
    state.insert(
        "current_session_duration_sec".into(),
        istr(current_session_duration_sec),
    );
    // Ensure emitted counters exist even if their event type never fired.
    state.insert("page_views".into(), istr(get_i64(&state, "page_views")));
    state.insert("clicks".into(), istr(get_i64(&state, "clicks")));
    state.insert("logins".into(), istr(get_i64(&state, "logins")));
    state.insert("feature_uses".into(), istr(get_i64(&state, "feature_uses")));
    state.insert("last_page".into(), json!(get_str(&state, "last_page").unwrap_or_default()));
    state.insert("last_country".into(), json!(get_str(&state, "last_country").unwrap_or_default()));
    state.insert("last_device".into(), json!(get_str(&state, "last_device").unwrap_or_default()));
    state.insert("last_browser".into(), json!(get_str(&state, "last_browser").unwrap_or_default()));
    // top_pages / top_features are emitted as JSON-array STRINGS (TEXT), per the
    // ARC-3 sink DDL (`top_pages TEXT`) and the ticket's explicit "extract as TEXT".
    // This DIVERGES from Flink's ProfileUpdate, which emits real JSON arrays, so the
    // Pinot `profiles` multi-value STRING columns will ingest these as a single
    // scalar string rather than as a multi-value list. Flagged for batch QA; a true
    // multi-value port would require declaring the sink column as a list type and
    // extracting with extract_json (List<Utf8>) instead of extract_json_string.
    state.insert("top_pages".into(), json!(serde_json::to_string(&top_pages).unwrap_or_else(|_| "[]".into())));
    state.insert("top_features".into(), json!(serde_json::to_string(&top_features).unwrap_or_else(|_| "[]".into())));
    state.insert("action".into(), json!(action));
    state.insert("changed_fields".into(), json!(changed_fields));
    state.insert("trigger".into(), json!("event"));

    Some(Value::Object(state).to_string())
}

// ----------------------------- helpers ---------------------------------

fn non_empty(v: Option<&str>) -> Option<&str> {
    v.filter(|s| !s.is_empty())
}

fn get_i64(state: &Map<String, Value>, key: &str) -> i64 {
    // Tolerant of BOTH JSON numbers and decimal STRINGS. Emitted numeric fields
    // are stored as strings (see `istr` / Step 16) so the SQL's
    // `extract_json_string` can read them, but those same keys double as internal
    // accumulators read back here on the next event; parsing strings keeps the
    // read-modify-write correct regardless of representation.
    state
        .get(key)
        .and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
        })
        .unwrap_or(0)
}

/// Encode an integer as a JSON STRING (e.g. `42` -> `"42"`). Emitted numeric and
/// timestamp output fields go through this so the profile-updater SQL's
/// `extract_json_string(blob, '$.field')` returns the value: that function yields
/// NULL on a JSON *number*, which is exactly what silently nulled every numeric
/// profile column before this fix. Mirrors how `current_session_active` is stored
/// as the string "true"/"false" for its `CAST(... AS BOOLEAN)`.
fn istr(n: i64) -> Value {
    json!(n.to_string())
}

fn get_str(state: &Map<String, Value>, key: &str) -> Option<String> {
    state
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}

fn bump_counter(state: &mut Map<String, Value>, key: &str) {
    let v = get_i64(state, key) + 1;
    state.insert(key.into(), json!(v));
}

fn bump_map(state: &mut Map<String, Value>, map_key: &str, sub_key: &str) {
    let mut m = match state.get(map_key) {
        Some(Value::Object(o)) => o.clone(),
        _ => Map::new(),
    };
    let cur = m.get(sub_key).and_then(Value::as_i64).unwrap_or(0) + 1;
    m.insert(sub_key.into(), json!(cur));
    state.insert(map_key.into(), Value::Object(m));
}

fn prune_daily(state: &mut Map<String, Value>, map_key: &str, event_day: i64) {
    let m = match state.get(map_key) {
        Some(Value::Object(o)) => o.clone(),
        _ => return,
    };
    // Reference day = later of this event's day and the newest key present, so
    // a single old/out-of-order event cannot wipe newer buckets.
    let mut reference_day = event_day;
    for k in m.keys() {
        reference_day = reference_day.max(days_from_civil_str(k));
    }
    let cutoff = reference_day - BUCKET_RETENTION_DAYS;
    let pruned: Map<String, Value> = m
        .into_iter()
        .filter(|(k, _)| days_from_civil_str(k) >= cutoff)
        .collect();
    state.insert(map_key.into(), Value::Object(pruned));
}

fn window_sums(state: &Map<String, Value>, map_key: &str, ref_day: i64) -> (i64, i64, i64, i64) {
    let m = match state.get(map_key) {
        Some(Value::Object(o)) => o,
        _ => return (0, 0, 0, 0),
    };
    let (mut d1, mut d7, mut d30, mut d90) = (0i64, 0i64, 0i64, 0i64);
    for (k, v) in m {
        let count = v.as_i64().unwrap_or(0);
        let day = days_from_civil_str(k);
        let delta = ref_day - day; // 0 == same day as the event
        if delta < 0 {
            continue; // future relative to this event; not in any window
        }
        // After the `delta < 0` guard, delta is non-negative, so `== 0`
        // (same day as the event) reads correctly and is equivalent to the old
        // `<= 0` while guarding against a future off-by-one.
        if delta == 0 {
            d1 += count;
        }
        if delta <= 6 {
            d7 += count;
        }
        if delta <= 29 {
            d30 += count;
        }
        if delta <= 89 {
            d90 += count;
        }
    }
    (d1, d7, d30, d90)
}

fn top_k(state: &Map<String, Value>, map_key: &str, k: usize) -> Vec<String> {
    let m = match state.get(map_key) {
        Some(Value::Object(o)) => o,
        _ => return Vec::new(),
    };
    let mut entries: Vec<(&String, i64)> = m
        .iter()
        .map(|(key, v)| (key, v.as_i64().unwrap_or(0)))
        .collect();
    // count desc, then key asc for determinism.
    entries.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));
    entries.into_iter().take(k).map(|(key, _)| key.clone()).collect()
}

fn wall_clock_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Clamp an event epoch-millis to the sane window, mirroring
/// ProfileFunction.clampEventTime (Java lines 44-48). Out-of-range timestamps
/// (far past / far future) collapse to wall-clock now so a single malformed
/// event cannot poison prune_daily's reference day or the rolling windows.
fn clamp_event_ms(event_ms: i64) -> i64 {
    let now = wall_clock_ms();
    if event_ms > now + MAX_FUTURE_MS {
        return now;
    }
    if event_ms < now - MAX_PAST_MS {
        return now;
    }
    event_ms
}

/// Parse "YYYY-MM-DD HH:mm:ss.SSS" (or ".SS" 2-digit-millis fallback) into
/// (epoch_millis_utc, "YYYY-MM-DD"), clamped to the sane event-time window.
/// On total parse failure, fall back to wall clock (mirrors
/// ProfileFunction.parseTimestamp). Both returned values are always derived
/// from the SAME clamped epoch, so the epoch and the date_key never diverge
/// (this also avoids the non-zero-padded raw-string edge case: we never take
/// the first 10 chars of the input).
fn parse_event_time(ts: &str) -> (i64, String) {
    let ms = clamp_event_ms(try_parse_event_time(ts).unwrap_or_else(wall_clock_ms));
    (ms, civil_from_days(ms.div_euclid(DAY_MS)))
}

fn try_parse_event_time(ts: &str) -> Option<i64> {
    let (date_part, time_part) = ts.split_once(' ')?;
    let mut dmy = date_part.split('-');
    let year: i64 = dmy.next()?.parse().ok()?;
    let month: i64 = dmy.next()?.parse().ok()?;
    let day: i64 = dmy.next()?.parse().ok()?;

    // time_part = "HH:mm:ss.SSS" (millis fractional may be ".SS" or absent).
    let (hms, frac) = match time_part.split_once('.') {
        Some((h, f)) => (h, f),
        None => (time_part, ""),
    };
    let mut hms_it = hms.split(':');
    let hour: i64 = hms_it.next()?.parse().ok()?;
    let minute: i64 = hms_it.next()?.parse().ok()?;
    let second: i64 = hms_it.next()?.parse().ok()?;

    // Normalize fractional seconds to millis. "" -> 0, "SS" -> *10, "SSS" -> as is.
    let millis: i64 = if frac.is_empty() {
        0
    } else {
        let f: i64 = frac.parse().ok()?;
        match frac.len() {
            1 => f * 100,
            2 => f * 10,
            3 => f,
            // Longer fractional: truncate to first 3 digits.
            _ => frac.get(..3)?.parse().ok()?,
        }
    };

    let days = days_from_civil(year, month, day);
    Some(days * DAY_MS + hour * 3_600_000 + minute * 60_000 + second * 1_000 + millis)
}

/// Howard Hinnant's days_from_civil: number of days since 1970-01-01 (UTC).
/// Valid for any Gregorian Y-M-D.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146097 + doe - 719468
}

/// Inverse of days_from_civil -> "YYYY-MM-DD". Used only for the wall-clock
/// fallback path.
fn civil_from_days(z: i64) -> String {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let y = if m <= 2 { y + 1 } else { y };
    format!("{:04}-{:02}-{:02}", y, m, d)
}

/// days_from_civil for a "YYYY-MM-DD" key. Returns i64::MIN on malformed keys
/// so they are treated as ancient and pruned (matches Java's parse-fail prune).
fn days_from_civil_str(date_key: &str) -> i64 {
    parse_civil_str(date_key).unwrap_or(i64::MIN)
}

fn parse_civil_str(date_key: &str) -> Option<i64> {
    let mut it = date_key.split('-');
    let y: i64 = it.next()?.parse().ok()?;
    let m: i64 = it.next()?.parse().ok()?;
    let d: i64 = it.next()?.parse().ok()?;
    Some(days_from_civil(y, m, d))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn extract(blob: &str, key: &str) -> Value {
        let v: Value = serde_json::from_str(blob).unwrap();
        v.get(key).cloned().unwrap_or(Value::Null)
    }

    #[test]
    fn days_from_civil_known_values() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(1970, 1, 2), 1);
        assert_eq!(days_from_civil(1969, 12, 31), -1);
        assert_eq!(days_from_civil(2000, 3, 1), 11017);
        // 2026-06-14
        assert_eq!(days_from_civil(2026, 6, 14), days_from_civil(1970, 1, 1) + 20618);
    }

    #[test]
    fn civil_from_days_roundtrips() {
        for &(y, m, d) in &[(1970, 1, 1), (2026, 6, 14), (1999, 12, 31), (2000, 1, 1)] {
            let days = days_from_civil(y, m, d);
            assert_eq!(civil_from_days(days), format!("{:04}-{:02}-{:02}", y, m, d));
        }
    }

    // These three exercise the raw parser directly. parse_event_time now clamps
    // far-past timestamps (1970 is well outside the 91-day window) to wall-clock
    // now, so we test try_parse_event_time to keep verifying the parse logic.
    #[test]
    fn parse_timestamp_millis() {
        // 1970-01-01 00:00:01.500 -> 1500 ms
        let ms = try_parse_event_time("1970-01-01 00:00:01.500").unwrap();
        assert_eq!(ms, 1500);
    }

    #[test]
    fn parse_timestamp_two_digit_millis_fallback() {
        // ".50" must mean 500 ms (2-digit-millis fallback like Java's .SS).
        let ms = try_parse_event_time("1970-01-01 00:00:00.50").unwrap();
        assert_eq!(ms, 500);
    }

    #[test]
    fn parse_timestamp_no_fraction() {
        let ms = try_parse_event_time("1970-01-01 00:00:02").unwrap();
        assert_eq!(ms, 2000);
    }

    #[test]
    fn parse_event_time_clamps_far_past() {
        // A far-past date (1970) is older than the 91-day window and must clamp
        // up to ~wall-clock now. date_key is re-derived from the clamped epoch.
        let (ms, key) = parse_event_time("1970-01-01 00:00:00.000");
        let floor = wall_clock_ms() - MAX_PAST_MS;
        assert!(
            ms >= floor,
            "far-past event should clamp to within the last 91 days: ms={ms} floor={floor}"
        );
        // date_key is consistent with the clamped epoch, not the raw "1970-...".
        assert_eq!(key, civil_from_days(ms.div_euclid(DAY_MS)));
        assert_ne!(key, "1970-01-01");
    }

    #[test]
    fn parse_event_time_clamps_far_future() {
        // A far-future date (2099) is beyond now + 60s and must clamp to <= now.
        let (ms, key) = parse_event_time("2099-01-01 00:00:00.000");
        let ceil = wall_clock_ms() + MAX_FUTURE_MS;
        assert!(
            ms <= ceil,
            "far-future event should clamp to <= now + epsilon: ms={ms} ceil={ceil}"
        );
        assert_eq!(key, civil_from_days(ms.div_euclid(DAY_MS)));
        assert_ne!(key, "2099-01-01");
    }

    #[test]
    fn window_sum_binning() {
        // 3 page_view events across 3 consecutive days.
        let mut blob: Option<String> = None;
        for day in &["2026-06-10", "2026-06-11", "2026-06-12"] {
            let out = profile_step(
                blob.as_deref(),
                "page_view",
                &format!("{} 12:00:00.000", day),
                Some("u1"),
                None,
                Some("/home"),
                None,
                None,
                None,
                None,
            )
            .unwrap();
            blob = Some(out);
        }
        let b = blob.unwrap();
        // Reference day is the last event (2026-06-12). Emitted numerics are
        // decimal STRINGS so the SQL's extract_json_string can read them.
        assert_eq!(extract(&b, "events_1d"), json!("1"));
        assert_eq!(extract(&b, "events_7d"), json!("3"));
        assert_eq!(extract(&b, "events_30d"), json!("3"));
        assert_eq!(extract(&b, "events_90d"), json!("3"));
        assert_eq!(extract(&b, "total_events"), json!("3"));
        assert_eq!(extract(&b, "page_views"), json!("3"));
    }

    #[test]
    fn session_transition_and_duration_accumulation() {
        // s1 spans 2026-06-10 12:00 -> 12:05 (5 min). Then s2 starts.
        let out1 = profile_step(
            None,
            "page_view",
            "2026-06-10 12:00:00.000",
            Some("u1"),
            Some("s1"),
            Some("/a"),
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(extract(&out1, "total_sessions"), json!("1"));
        assert_eq!(extract(&out1, "current_session_active"), json!("true"));

        // second event in s1, 5 minutes later -> advances last_seen.
        let out2 = profile_step(
            Some(&out1),
            "page_view",
            "2026-06-10 12:05:00.000",
            Some("u1"),
            Some("s1"),
            Some("/b"),
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(extract(&out2, "total_sessions"), json!("1"));
        assert_eq!(extract(&out2, "current_session_duration_sec"), json!("300"));

        // s2 starts -> closes s1, accumulating 300s = 300000ms over 1 closed.
        let out3 = profile_step(
            Some(&out2),
            "page_view",
            "2026-06-10 12:10:00.000",
            Some("u1"),
            Some("s2"),
            Some("/c"),
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(extract(&out3, "total_sessions"), json!("2"));
        // avg = 300000 / 1 / 1000 = 300 sec
        assert_eq!(extract(&out3, "avg_session_duration_sec"), json!("300"));
        assert_eq!(extract(&out3, "current_session_active"), json!("true"));
    }

    #[test]
    fn top_k_ordering_deterministic() {
        let mut blob: Option<String> = None;
        // /a x3, /b x2, /c x1 ; tie-break by key asc only on equal counts.
        let pages = ["/a", "/a", "/a", "/b", "/b", "/c"];
        for p in &pages {
            let out = profile_step(
                blob.as_deref(),
                "page_view",
                "2026-06-10 12:00:00.000",
                Some("u1"),
                None,
                Some(p),
                None,
                None,
                None,
                None,
            )
            .unwrap();
            blob = Some(out);
        }
        let b = blob.unwrap();
        let top: Vec<String> =
            serde_json::from_str(extract(&b, "top_pages").as_str().unwrap()).unwrap();
        assert_eq!(top, vec!["/a", "/b", "/c"]);
    }

    #[test]
    fn action_create_then_update() {
        let out1 = profile_step(
            None,
            "page_view",
            "2026-06-10 12:00:00.000",
            Some("u1"),
            None,
            Some("/a"),
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(extract(&out1, "action"), json!("create"));
        // first_seen == last_seen on the create event (both decimal strings).
        assert_eq!(extract(&out1, "first_seen"), extract(&out1, "last_seen"));

        let out2 = profile_step(
            Some(&out1),
            "click",
            "2026-06-10 12:01:00.000",
            Some("u1"),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(extract(&out2, "action"), json!("update"));
        assert_eq!(extract(&out2, "clicks"), json!("1"));
        assert_eq!(extract(&out2, "total_events"), json!("2"));
    }

    #[test]
    fn user_id_persists_when_event_has_none() {
        let out1 = profile_step(
            None,
            "page_view",
            "2026-06-10 12:00:00.000",
            Some("user-42"),
            None,
            Some("/a"),
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(extract(&out1, "user_id"), json!("user-42"));

        // anonymous follow-up event keeps the stored user_id.
        let out2 = profile_step(
            Some(&out1),
            "click",
            "2026-06-10 12:01:00.000",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(extract(&out2, "user_id"), json!("user-42"));
    }

    // Regression for the dashboard "empty profiles + 0 users" bug: every emitted
    // numeric/timestamp field MUST be a JSON string that parses to an integer.
    // The profile-updater SQL reads them with extract_json_string, which returns
    // NULL on a JSON number; storing them as numbers nulled every numeric profile
    // column downstream (flaredb -> query-api drops the row / counts 0).
    #[test]
    fn emitted_numeric_fields_are_integer_strings() {
        let out = profile_step(
            None,
            "page_view",
            "2026-06-10 12:00:00.000",
            Some("u1"),
            Some("s1"),
            Some("/a"),
            None,
            None,
            None,
            None,
        )
        .unwrap();
        let v: Value = serde_json::from_str(&out).unwrap();

        for key in [
            "first_seen",
            "last_seen",
            "updated_at",
            "total_events",
            "total_sessions",
            "events_1d",
            "events_7d",
            "events_30d",
            "events_90d",
            "sessions_1d",
            "sessions_7d",
            "sessions_30d",
            "sessions_90d",
            "avg_session_duration_sec",
            "current_session_duration_sec",
            "page_views",
            "clicks",
            "logins",
            "feature_uses",
        ] {
            let field = &v[key];
            assert!(
                field.is_string(),
                "{key} must be a JSON string for extract_json_string; got {field:?}"
            );
            assert!(
                field.as_str().unwrap().parse::<i64>().is_ok(),
                "{key} must be a decimal integer string; got {field:?}"
            );
        }

        // first_seen/total_events read back correctly on the NEXT event (proves
        // get_i64 parses the string-encoded state, so accumulation is unaffected).
        let out2 = profile_step(
            Some(&out),
            "page_view",
            "2026-06-10 12:01:00.000",
            Some("u1"),
            Some("s1"),
            Some("/b"),
            None,
            None,
            None,
            None,
        )
        .unwrap();
        let v2: Value = serde_json::from_str(&out2).unwrap();
        assert_eq!(v2["total_events"], json!("2"));
        // first_seen unchanged from the create event; last_seen advanced.
        assert_eq!(v2["first_seen"], v["first_seen"]);
        assert_ne!(v2["last_seen"], v["last_seen"]);
    }
}
