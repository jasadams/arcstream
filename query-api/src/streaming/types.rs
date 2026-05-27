use async_graphql::SimpleObject;
use serde::{Deserialize, Serialize};

use crate::schema::live_profile::LiveProfile;

#[derive(SimpleObject, Clone, Debug, Serialize, Deserialize)]
pub struct ProfileUpdateMessage {
    pub canonical_id: String,
    pub tenant_id: String,
    pub profile: LiveProfile,
    pub changed_fields: Vec<String>,
    pub timestamp: String,
    pub trigger: String,
    pub action: String,
}

#[derive(Deserialize)]
pub struct FlatProfileUpdate {
    pub canonical_id: String,
    pub tenant_id: String,
    #[serde(default)]
    pub user_id: String,
    #[serde(default)]
    pub first_seen: u64,
    #[serde(default)]
    pub last_seen: u64,
    #[serde(default)]
    pub updated_at: u64,
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
    pub sessions_30d: u64,
    #[serde(default)]
    pub sessions_90d: u64,
    #[serde(default)]
    pub avg_session_duration_sec: u64,
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
    #[serde(default)]
    pub action: String,
    #[serde(default)]
    pub changed_fields: Vec<String>,
    #[serde(default)]
    pub timestamp: String,
    #[serde(default)]
    pub trigger: String,
}

fn format_epoch_millis(ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(ms as i64)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string())
        .unwrap_or_default()
}

impl FlatProfileUpdate {
    pub fn into_message(self) -> ProfileUpdateMessage {
        ProfileUpdateMessage {
            canonical_id: self.canonical_id.clone(),
            tenant_id: self.tenant_id.clone(),
            profile: LiveProfile {
                canonical_id: self.canonical_id,
                user_id: self.user_id,
                tenant_id: self.tenant_id,
                first_seen: format_epoch_millis(self.first_seen),
                last_seen: format_epoch_millis(self.last_seen),
                total_events: self.total_events,
                total_sessions: self.total_sessions,
                events_1d: self.events_1d,
                events_7d: self.events_7d,
                events_30d: self.events_30d,
                events_90d: self.events_90d,
                sessions_1d: self.sessions_1d,
                sessions_7d: self.sessions_7d,
                avg_session_duration_sec: self.avg_session_duration_sec,
                current_session_active: false,
                current_session_duration_sec: 0,
                page_views: self.page_views,
                clicks: self.clicks,
                logins: self.logins,
                feature_uses: self.feature_uses,
                last_page: self.last_page,
                last_country: self.last_country,
                last_device: self.last_device,
                last_browser: self.last_browser,
                top_pages: self.top_pages,
                top_features: self.top_features,
            },
            changed_fields: self.changed_fields,
            timestamp: self.timestamp,
            trigger: self.trigger,
            action: self.action,
        }
    }
}

#[derive(SimpleObject, Clone, Debug, Serialize, Deserialize)]
pub struct LiveEventMessage {
    pub event_id: String,
    pub event_type: String,
    pub tenant_id: String,
    pub event_time: String,
    pub canonical_id: String,
    #[serde(default)]
    pub anonymous_id: String,
    #[serde(default)]
    pub user_id: String,
    #[serde(default)]
    pub page_url: String,
    #[serde(default)]
    pub device_type: String,
    #[serde(default)]
    pub browser: String,
    #[serde(default)]
    pub country: String,
}
