use async_graphql::SimpleObject;
use serde::{Deserialize, Serialize};

#[derive(SimpleObject, Clone, Debug, Serialize, Deserialize)]
pub struct LiveProfile {
    pub canonical_id: String,
    #[serde(default)]
    pub user_id: String,
    #[serde(default)]
    pub tenant_id: String,
    #[serde(default)]
    pub first_seen: String,
    #[serde(default)]
    pub last_seen: String,
    #[serde(default)]
    pub total_events: u64,
    #[serde(default)]
    pub total_sessions: u64,
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
    pub avg_session_duration_sec: u64,
    #[serde(default)]
    pub current_session_active: bool,
    #[serde(default)]
    pub current_session_duration_sec: u64,
    #[serde(default)]
    pub page_views: u64,
    #[serde(default)]
    pub clicks: u64,
    #[serde(default)]
    pub logins: u64,
    #[serde(default)]
    pub feature_uses: u64,
    #[serde(default)]
    pub last_page: String,
    #[serde(default)]
    pub last_country: String,
    #[serde(default)]
    pub last_device: String,
    #[serde(default)]
    pub last_browser: String,
    #[serde(default)]
    pub top_pages: Vec<String>,
    #[serde(default)]
    pub top_features: Vec<String>,
}
