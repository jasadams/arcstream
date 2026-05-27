use leptos::prelude::*;
use serde::{Deserialize, Serialize};
#[cfg(feature = "ssr")]
use crate::util::PAGE_SIZE;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UserProfile {
    #[serde(alias = "tenantId")]
    pub tenant_id: String,
    #[serde(alias = "canonicalId")]
    pub canonical_id: String,
    #[serde(alias = "firstSeen")]
    pub first_seen: String,
    #[serde(alias = "lastSeen")]
    pub last_seen: String,
    #[serde(alias = "totalEvents")]
    pub total_events: u64,
    #[serde(alias = "totalSessions")]
    pub total_sessions: u64,
    #[serde(default, alias = "pageViews")]
    pub page_views: u64,
    #[serde(default)]
    pub clicks: u64,
    #[serde(default)]
    pub signups: u64,
    #[serde(default)]
    pub logins: u64,
    #[serde(default, alias = "featureUses")]
    pub feature_uses: u64,
    #[serde(alias = "lastCountry")]
    pub last_country: String,
    #[serde(alias = "lastDevice")]
    pub last_device: String,
    #[serde(alias = "lastBrowser")]
    pub last_browser: String,
    #[serde(default, alias = "events1D")]
    pub events_1d: u64,
    #[serde(default, alias = "events7D")]
    pub events_7d: u64,
    #[serde(default, alias = "events30D")]
    pub events_30d: u64,
    #[serde(default, alias = "events90D")]
    pub events_90d: u64,
    #[serde(default, alias = "sessions1D")]
    pub sessions_1d: u64,
    #[serde(default, alias = "sessions7D")]
    pub sessions_7d: u64,
    #[serde(default, alias = "sessions30D")]
    pub sessions_30d: u64,
    #[serde(default, alias = "sessions90D")]
    pub sessions_90d: u64,
    #[serde(default, alias = "totalClosedSessions")]
    pub total_closed_sessions: u64,
    #[serde(default, alias = "avgSessionDurationSec")]
    pub avg_session_duration_sec: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventRow {
    #[serde(alias = "eventId")]
    pub event_id: String,
    #[serde(alias = "eventType")]
    pub event_type: String,
    #[serde(alias = "tenantId")]
    pub tenant_id: String,
    #[serde(alias = "eventTime")]
    pub event_time: String,
    #[serde(alias = "canonicalId")]
    pub canonical_id: String,
    #[serde(alias = "anonymousId")]
    pub anonymous_id: String,
    #[serde(alias = "userId")]
    pub user_id: String,
    #[serde(alias = "pageUrl")]
    pub page_url: String,
    #[serde(alias = "deviceType")]
    pub device_type: String,
    pub browser: String,
    pub country: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LiveProfile {
    #[serde(alias = "canonicalId")]
    pub canonical_id: String,
    #[serde(default, alias = "userId")]
    pub user_id: String,
    #[serde(default, alias = "tenantId")]
    pub tenant_id: String,
    #[serde(default, alias = "firstSeen")]
    pub first_seen: String,
    #[serde(default, alias = "lastSeen")]
    pub last_seen: String,
    #[serde(default, alias = "totalEvents")]
    pub total_events: u64,
    #[serde(default, alias = "totalSessions")]
    pub total_sessions: u64,
    #[serde(default, alias = "sessions1D", alias = "sessions1d")]
    pub sessions_1d: u64,
    #[serde(default, alias = "sessions7D", alias = "sessions7d")]
    pub sessions_7d: u64,
    #[serde(default, alias = "events1D", alias = "events1d")]
    pub events_1d: u64,
    #[serde(default, alias = "events7D", alias = "events7d")]
    pub events_7d: u64,
    #[serde(default, alias = "events30D", alias = "events30d")]
    pub events_30d: u64,
    #[serde(default, alias = "events90D", alias = "events90d")]
    pub events_90d: u64,
    #[serde(default, alias = "avgSessionDurationSec")]
    pub avg_session_duration_sec: u64,
    #[serde(default, alias = "currentSessionActive")]
    pub current_session_active: bool,
    #[serde(default, alias = "currentSessionDurationSec")]
    pub current_session_duration_sec: u64,
    #[serde(default, alias = "pageViews")]
    pub page_views: u64,
    #[serde(default)]
    pub clicks: u64,
    #[serde(default)]
    pub logins: u64,
    #[serde(default, alias = "featureUses")]
    pub feature_uses: u64,
    #[serde(default, alias = "lastPage")]
    pub last_page: String,
    #[serde(default, alias = "lastCountry")]
    pub last_country: String,
    #[serde(default, alias = "lastDevice")]
    pub last_device: String,
    #[serde(default, alias = "lastBrowser")]
    pub last_browser: String,
    #[serde(default, alias = "topPages")]
    pub top_pages: Vec<String>,
    #[serde(default, alias = "topFeatures")]
    pub top_features: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UserCount {
    pub total: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DashboardStats {
    #[serde(alias = "totalUsers")]
    pub total_users: u64,
    #[serde(alias = "totalEvents")]
    pub total_events: u64,
    #[serde(alias = "activeSessions")]
    pub active_sessions: u64,
}

#[server(GetUsers)]
pub async fn get_users(page: u32) -> Result<Vec<UserProfile>, ServerFnError> {
    use crate::server::AppState;
    use crate::server::query_api::graphql_query;

    let state = expect_context::<AppState>();
    let limit = PAGE_SIZE as i32;
    let offset = (page * PAGE_SIZE) as i32;

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct UsersConnection {
        nodes: Vec<UserProfile>,
    }
    #[derive(Deserialize)]
    struct Response {
        users: UsersConnection,
    }

    let vars = serde_json::json!({ "limit": limit, "offset": offset });
    let data: Response = graphql_query(
        &state,
        "query($limit: Int!, $offset: Int!) { users(limit: $limit, offset: $offset) { nodes { \
            tenantId canonicalId firstSeen lastSeen \
            totalEvents totalSessions pageViews clicks signups logins featureUses \
            lastCountry lastDevice lastBrowser \
            events1D events7D events30D events90D \
            sessions1D sessions7D sessions30D sessions90D \
            totalClosedSessions avgSessionDurationSec \
        } } }",
        vars,
    )
    .await
    .map_err(ServerFnError::new)?;

    Ok(data.users.nodes)
}

#[server(GetUserCount)]
pub async fn get_user_count() -> Result<UserCount, ServerFnError> {
    use crate::server::AppState;
    use crate::server::query_api::graphql_query;

    let state = expect_context::<AppState>();

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Stats {
        total_users: u64,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Response {
        dashboard_stats: Stats,
    }

    let data: Response = graphql_query(
        &state,
        "{ dashboardStats { totalUsers } }",
        serde_json::json!({}),
    )
    .await
    .map_err(ServerFnError::new)?;

    Ok(UserCount { total: data.dashboard_stats.total_users })
}

#[server(GetDashboardStats)]
pub async fn get_dashboard_stats() -> Result<DashboardStats, ServerFnError> {
    use crate::server::AppState;
    use crate::server::query_api::graphql_query;

    let state = expect_context::<AppState>();

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Response {
        dashboard_stats: DashboardStats,
    }

    let data: Response = graphql_query(
        &state,
        "{ dashboardStats { totalUsers totalEvents activeSessions } }",
        serde_json::json!({}),
    )
    .await
    .map_err(ServerFnError::new)?;

    Ok(data.dashboard_stats)
}

#[server(GetLiveProfile)]
pub async fn get_live_profile(tenant_id: String, canonical_id: String) -> Result<LiveProfile, ServerFnError> {
    use crate::server::AppState;
    use crate::server::query_api::graphql_query;

    let state = expect_context::<AppState>();

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Response {
        live_profile: Option<LiveProfile>,
    }

    let vars = serde_json::json!({
        "tenantId": tenant_id,
        "canonicalId": canonical_id,
    });
    let data: Response = graphql_query(
        &state,
        "query($tenantId: String!, $canonicalId: String!) { \
            liveProfile(tenantId: $tenantId, canonicalId: $canonicalId) { \
                canonicalId userId tenantId firstSeen lastSeen \
                totalEvents totalSessions \
                events1D events7D events30D events90D \
                sessions1D sessions7D \
                avgSessionDurationSec currentSessionActive currentSessionDurationSec \
                pageViews clicks logins featureUses \
                lastPage lastCountry lastDevice lastBrowser \
                topPages topFeatures \
            } \
        }",
        vars,
    )
    .await
    .map_err(ServerFnError::new)?;

    data.live_profile.ok_or_else(|| ServerFnError::new("Profile not found"))
}

#[server(GetEvents)]
pub async fn get_events(tenant_id: String, canonical_id: String) -> Result<Vec<EventRow>, ServerFnError> {
    use crate::server::AppState;
    use crate::server::query_api::graphql_query;

    let state = expect_context::<AppState>();

    #[derive(Deserialize)]
    struct TenantData {
        events: Vec<EventRow>,
    }
    #[derive(Deserialize)]
    struct Response {
        tenant: Option<TenantData>,
    }

    let vars = serde_json::json!({
        "tenantId": tenant_id,
        "canonicalId": canonical_id,
    });
    let data: Response = graphql_query(
        &state,
        "query($tenantId: String!, $canonicalId: String!) { \
            tenant(id: $tenantId) { \
                events(canonicalId: $canonicalId, limit: 50) { \
                    eventId eventType tenantId eventTime canonicalId \
                    anonymousId userId pageUrl deviceType browser country \
                } \
            } \
        }",
        vars,
    )
    .await
    .map_err(ServerFnError::new)?;

    Ok(data.tenant.map(|t| t.events).unwrap_or_default())
}

#[server(GetAllEvents)]
pub async fn get_all_events(
    event_type: Option<String>,
    device_type: Option<String>,
) -> Result<Vec<EventRow>, ServerFnError> {
    use crate::server::AppState;
    use crate::server::query_api::graphql_query;

    let state = expect_context::<AppState>();

    #[derive(Deserialize)]
    struct Response {
        events: Vec<EventRow>,
    }

    let vars = serde_json::json!({
        "eventType": event_type,
        "deviceType": device_type,
    });
    let data: Response = graphql_query(
        &state,
        "query($eventType: String, $deviceType: String) { \
            events(eventType: $eventType, deviceType: $deviceType) { \
                eventId eventType tenantId eventTime canonicalId \
                anonymousId userId pageUrl deviceType browser country \
            } \
        }",
        vars,
    )
    .await
    .map_err(ServerFnError::new)?;

    Ok(data.events)
}

#[server(GetEvent)]
pub async fn get_event(tenant_id: String, event_id: String) -> Result<EventRow, ServerFnError> {
    use crate::server::AppState;
    use crate::server::query_api::graphql_query;

    let state = expect_context::<AppState>();

    #[derive(Deserialize)]
    struct Response {
        event: Option<EventRow>,
    }

    let vars = serde_json::json!({
        "tenantId": tenant_id,
        "eventId": event_id,
    });
    let data: Response = graphql_query(
        &state,
        "query($tenantId: String!, $eventId: String!) { \
            event(tenantId: $tenantId, eventId: $eventId) { \
                eventId eventType tenantId eventTime canonicalId \
                anonymousId userId pageUrl deviceType browser country \
            } \
        }",
        vars,
    )
    .await
    .map_err(ServerFnError::new)?;

    data.event.ok_or_else(|| ServerFnError::new("Event not found"))
}
