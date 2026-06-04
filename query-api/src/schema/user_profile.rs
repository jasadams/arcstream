use async_graphql::{Context, Object, SimpleObject};
use serde::Deserialize;
use std::sync::Arc;

use crate::db::pinot::{parse_jsonl, sanitize_input, PinotQuerier};
use crate::db::LiveProfileProvider;
use crate::schema::event::Event;
use crate::schema::live_profile::LiveProfile;

#[derive(SimpleObject)]
pub struct UserConnection {
    pub nodes: Vec<UserProfile>,
    pub total_count: u64,
}

#[derive(Deserialize)]
pub struct UserProfileRow {
    pub tenant_id: String,
    pub canonical_id: String,
    pub first_seen: String,
    pub last_seen: String,
    pub total_events: u64,
    pub total_sessions: u64,
    #[serde(default)]
    pub page_views: u64,
    #[serde(default)]
    pub clicks: u64,
    #[serde(default)]
    pub signups: u64,
    #[serde(default)]
    pub logins: u64,
    #[serde(default)]
    pub feature_uses: u64,
    pub last_country: String,
    pub last_device: String,
    pub last_browser: String,
    #[serde(default)]
    pub events_1d: u64,
    #[serde(default)]
    pub events_7d: u64,
    #[serde(default)]
    pub events_30d: u64,
    #[serde(default)]
    pub events_90d: u64,
    #[serde(default)]
    pub sessions_1d: u64,
    #[serde(default)]
    pub sessions_7d: u64,
    #[serde(default)]
    pub sessions_30d: u64,
    #[serde(default)]
    pub sessions_90d: u64,
    #[serde(default)]
    pub total_closed_sessions: u64,
    #[serde(default)]
    pub avg_session_duration_sec: u64,
}

pub struct UserProfile {
    pub tenant_id: String,
    pub canonical_id: String,
    pub first_seen: String,
    pub last_seen: String,
    pub total_events: u64,
    pub total_sessions: u64,
    pub page_views: u64,
    pub clicks: u64,
    pub signups: u64,
    pub logins: u64,
    pub feature_uses: u64,
    pub events_1d: u64,
    pub events_7d: u64,
    pub events_30d: u64,
    pub events_90d: u64,
    pub sessions_1d: u64,
    pub sessions_7d: u64,
    pub sessions_30d: u64,
    pub sessions_90d: u64,
    pub total_closed_sessions: u64,
    pub avg_session_duration_sec: u64,
    pub last_country: String,
    pub last_device: String,
    pub last_browser: String,
}

impl From<UserProfileRow> for UserProfile {
    fn from(row: UserProfileRow) -> Self {
        Self {
            tenant_id: row.tenant_id,
            canonical_id: row.canonical_id,
            first_seen: row.first_seen,
            last_seen: row.last_seen,
            total_events: row.total_events,
            total_sessions: row.total_sessions,
            page_views: row.page_views,
            clicks: row.clicks,
            signups: row.signups,
            logins: row.logins,
            feature_uses: row.feature_uses,
            events_1d: row.events_1d,
            events_7d: row.events_7d,
            events_30d: row.events_30d,
            events_90d: row.events_90d,
            sessions_1d: row.sessions_1d,
            sessions_7d: row.sessions_7d,
            sessions_30d: row.sessions_30d,
            sessions_90d: row.sessions_90d,
            total_closed_sessions: row.total_closed_sessions,
            avg_session_duration_sec: row.avg_session_duration_sec,
            last_country: row.last_country,
            last_device: row.last_device,
            last_browser: row.last_browser,
        }
    }
}

#[Object]
impl UserProfile {
    async fn canonical_id(&self) -> &str {
        &self.canonical_id
    }
    async fn tenant_id(&self) -> &str {
        &self.tenant_id
    }
    async fn first_seen(&self) -> &str {
        &self.first_seen
    }
    async fn last_seen(&self) -> &str {
        &self.last_seen
    }
    async fn total_events(&self) -> u64 {
        self.total_events
    }
    async fn total_sessions(&self) -> u64 {
        self.total_sessions
    }
    async fn page_views(&self) -> u64 {
        self.page_views
    }
    async fn clicks(&self) -> u64 {
        self.clicks
    }
    async fn signups(&self) -> u64 {
        self.signups
    }
    async fn logins(&self) -> u64 {
        self.logins
    }
    async fn feature_uses(&self) -> u64 {
        self.feature_uses
    }
    async fn events_1d(&self) -> u64 {
        self.events_1d
    }
    async fn events_7d(&self) -> u64 {
        self.events_7d
    }
    async fn events_30d(&self) -> u64 {
        self.events_30d
    }
    async fn events_90d(&self) -> u64 {
        self.events_90d
    }
    async fn sessions_1d(&self) -> u64 {
        self.sessions_1d
    }
    async fn sessions_7d(&self) -> u64 {
        self.sessions_7d
    }
    async fn sessions_30d(&self) -> u64 {
        self.sessions_30d
    }
    async fn sessions_90d(&self) -> u64 {
        self.sessions_90d
    }
    async fn total_closed_sessions(&self) -> u64 {
        self.total_closed_sessions
    }
    async fn avg_session_duration_sec(&self) -> u64 {
        self.avg_session_duration_sec
    }
    async fn last_country(&self) -> &str {
        &self.last_country
    }
    async fn last_device(&self) -> &str {
        &self.last_device
    }
    async fn last_browser(&self) -> &str {
        &self.last_browser
    }

    async fn live_profile(&self, ctx: &Context<'_>) -> async_graphql::Result<Option<LiveProfile>> {
        let profile_store = ctx.data::<Arc<dyn LiveProfileProvider>>()?;
        let value = profile_store
            .get_live_profile(&self.tenant_id, &self.canonical_id)
            .await
            .map_err(async_graphql::Error::new)?;

        match value {
            Some(json) => {
                let profile: LiveProfile =
                    serde_json::from_str(&json).map_err(|e| async_graphql::Error::new(e.to_string()))?;
                Ok(Some(profile))
            }
            None => Ok(None),
        }
    }

    async fn recent_events(
        &self,
        ctx: &Context<'_>,
        #[graphql(default = 50)] limit: i32,
    ) -> async_graphql::Result<Vec<Event>> {
        let pinot = ctx.data::<Arc<dyn PinotQuerier>>()?;
        let safe_id = sanitize_input(&self.canonical_id)
            .map_err(async_graphql::Error::new)?;
        let safe_tenant = sanitize_input(&self.tenant_id)
            .map_err(async_graphql::Error::new)?;
        let limit = limit.clamp(1, 200) as u32;

        let sql = format!(
            "SELECT event_id, event_type, tenant_id, event_time, canonical_id, \
             anonymous_id, user_id, page_url, device_type, browser, country \
             FROM events \
             WHERE tenant_id = '{safe_tenant}' AND canonical_id = '{safe_id}' \
             ORDER BY event_time DESC LIMIT {limit}"
        );

        let body = pinot.query(&sql).await.map_err(async_graphql::Error::new)?;
        Ok(parse_jsonl(&body))
    }
}
