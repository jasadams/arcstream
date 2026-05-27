use async_trait::async_trait;
use scylla::DeserializeRow;
use scylla::frame::value::CqlTimestamp;
use std::sync::Arc;

#[async_trait]
pub trait LiveProfileProvider: Send + Sync {
    async fn get_live_profile(
        &self,
        tenant_id: &str,
        canonical_id: &str,
    ) -> Result<Option<String>, String>;
}

pub struct ScyllaClient {
    pub session: Arc<scylla::Session>,
}

#[derive(DeserializeRow)]
struct ProfileRow {
    tenant_id: String,
    canonical_id: String,
    user_id: Option<String>,
    first_seen: Option<CqlTimestamp>,
    last_seen: Option<CqlTimestamp>,
    total_events: Option<i64>,
    total_sessions: Option<i64>,
    events_1d: Option<i64>,
    events_7d: Option<i64>,
    events_30d: Option<i64>,
    events_90d: Option<i64>,
    sessions_1d: Option<i64>,
    sessions_7d: Option<i64>,
    sessions_30d: Option<i64>,
    sessions_90d: Option<i64>,
    avg_session_duration_sec: Option<i64>,
    current_session_active: Option<bool>,
    current_session_duration_sec: Option<i64>,
    page_views: Option<i64>,
    clicks: Option<i64>,
    logins: Option<i64>,
    feature_uses: Option<i64>,
    last_page: Option<String>,
    last_country: Option<String>,
    last_device: Option<String>,
    last_browser: Option<String>,
    top_pages: Option<Vec<String>>,
    top_features: Option<Vec<String>>,
}

fn format_timestamp(ts: Option<CqlTimestamp>) -> String {
    match ts {
        Some(CqlTimestamp(millis)) => chrono::DateTime::from_timestamp_millis(millis)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string())
            .unwrap_or_default(),
        None => String::new(),
    }
}

#[async_trait]
impl LiveProfileProvider for ScyllaClient {
    async fn get_live_profile(
        &self,
        tenant_id: &str,
        canonical_id: &str,
    ) -> Result<Option<String>, String> {
        let result = self
            .session
            .query_unpaged(
                "SELECT tenant_id, canonical_id, user_id, first_seen, last_seen, \
                 total_events, total_sessions, events_1d, events_7d, events_30d, events_90d, \
                 sessions_1d, sessions_7d, sessions_30d, sessions_90d, \
                 avg_session_duration_sec, current_session_active, current_session_duration_sec, \
                 page_views, clicks, logins, feature_uses, \
                 last_page, last_country, last_device, last_browser, \
                 top_pages, top_features \
                 FROM cdp.profiles WHERE tenant_id = ? AND canonical_id = ?",
                (tenant_id, canonical_id),
            )
            .await
            .map_err(|e| format!("ScyllaDB query failed: {e}"))?;

        let rows_result = result
            .into_rows_result()
            .map_err(|e| format!("Failed to parse rows: {e}"))?;

        let row = rows_result
            .maybe_first_row::<ProfileRow>()
            .map_err(|e| format!("Failed to deserialize profile: {e}"))?;

        match row {
            None => Ok(None),
            Some(r) => {
                let json = serde_json::json!({
                    "canonical_id": r.canonical_id,
                    "user_id": r.user_id.unwrap_or_default(),
                    "tenant_id": r.tenant_id,
                    "first_seen": format_timestamp(r.first_seen),
                    "last_seen": format_timestamp(r.last_seen),
                    "total_events": r.total_events.unwrap_or(0),
                    "total_sessions": r.total_sessions.unwrap_or(0),
                    "events_1d": r.events_1d.unwrap_or(0),
                    "events_7d": r.events_7d.unwrap_or(0),
                    "events_30d": r.events_30d.unwrap_or(0),
                    "events_90d": r.events_90d.unwrap_or(0),
                    "sessions_1d": r.sessions_1d.unwrap_or(0),
                    "sessions_7d": r.sessions_7d.unwrap_or(0),
                    "sessions_30d": r.sessions_30d.unwrap_or(0),
                    "sessions_90d": r.sessions_90d.unwrap_or(0),
                    "avg_session_duration_sec": r.avg_session_duration_sec.unwrap_or(0),
                    "current_session_active": r.current_session_active.unwrap_or(false),
                    "current_session_duration_sec": r.current_session_duration_sec.unwrap_or(0),
                    "page_views": r.page_views.unwrap_or(0),
                    "clicks": r.clicks.unwrap_or(0),
                    "logins": r.logins.unwrap_or(0),
                    "feature_uses": r.feature_uses.unwrap_or(0),
                    "last_page": r.last_page.unwrap_or_default(),
                    "last_country": r.last_country.unwrap_or_default(),
                    "last_device": r.last_device.unwrap_or_default(),
                    "last_browser": r.last_browser.unwrap_or_default(),
                    "top_pages": r.top_pages.unwrap_or_default(),
                    "top_features": r.top_features.unwrap_or_default(),
                });
                serde_json::to_string(&json)
                    .map(Some)
                    .map_err(|e| format!("JSON serialization failed: {e}"))
            }
        }
    }
}
