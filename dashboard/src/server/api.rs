use leptos::prelude::*;
use serde::{Deserialize, Serialize};
use crate::server::WithStats;
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
    #[serde(default, alias = "pageViews")]
    pub page_views: u64,
    #[serde(default)]
    pub clicks: u64,
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
    #[serde(default, alias = "avgSessionDurationSec")]
    pub avg_session_duration_sec: Option<f64>,
}

impl From<&LiveProfile> for UserProfile {
    fn from(profile: &LiveProfile) -> Self {
        Self {
            tenant_id: profile.tenant_id.clone(), canonical_id: profile.canonical_id.clone(),
            first_seen: profile.first_seen.clone(), last_seen: profile.last_seen.clone(),
            total_events: profile.total_events, page_views: profile.page_views,
            clicks: profile.clicks, logins: profile.logins, feature_uses: profile.feature_uses,
            last_country: profile.last_country.clone(), last_device: profile.last_device.clone(),
            last_browser: profile.last_browser.clone(), events_1d: profile.events_1d,
            events_7d: profile.events_7d, events_30d: profile.events_30d,
            events_90d: profile.events_90d, avg_session_duration_sec: profile.avg_session_duration_sec,
        }
    }
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
    #[serde(default, alias = "events1D", alias = "events1d")]
    pub events_1d: u64,
    #[serde(default, alias = "events7D", alias = "events7d")]
    pub events_7d: u64,
    #[serde(default, alias = "events30D", alias = "events30d")]
    pub events_30d: u64,
    #[serde(default, alias = "events90D", alias = "events90d")]
    pub events_90d: u64,
    #[serde(default, alias = "avgSessionDurationSec")]
    pub avg_session_duration_sec: Option<f64>,
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
}

#[server(GetUsers)]
pub async fn get_users(page: u32) -> Result<WithStats<Vec<UserProfile>>, ServerFnError> {
    use crate::server::AppState;
    use crate::server::query_api::graphql_query_with_stats;

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
    let (data, stats): (Response, Vec<crate::server::QueryStatEntry>) = graphql_query_with_stats(
        &state,
        "query($limit: Int!, $offset: Int!) { users(limit: $limit, offset: $offset) { nodes { \
            tenantId canonicalId firstSeen lastSeen \
            totalEvents pageViews clicks logins featureUses \
            lastCountry lastDevice lastBrowser \
            events1D events7D events30D events90D \
            \
            avgSessionDurationSec \
        } } }",
        vars,
    )
    .await
    .map_err(ServerFnError::new)?;

    Ok(WithStats { data: data.users.nodes, stats })
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
pub async fn get_dashboard_stats() -> Result<WithStats<DashboardStats>, ServerFnError> {
    use crate::server::AppState;
    use crate::server::query_api::graphql_query_with_stats;

    let state = expect_context::<AppState>();

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Response {
        dashboard_stats: DashboardStats,
    }

    let (data, stats): (Response, Vec<crate::server::QueryStatEntry>) = graphql_query_with_stats(
        &state,
        "{ dashboardStats { totalUsers totalEvents } }",
        serde_json::json!({}),
    )
    .await
    .map_err(ServerFnError::new)?;

    Ok(WithStats { data: data.dashboard_stats, stats })
}

#[server(GetLiveProfile)]
pub async fn get_live_profile(tenant_id: String, canonical_id: String) -> Result<Option<LiveProfile>, ServerFnError> {
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
                totalEvents \
                events1D events7D events30D events90D \
                \
                avgSessionDurationSec \
                pageViews clicks logins featureUses \
                lastPage lastCountry lastDevice lastBrowser \
                topPages topFeatures \
            } \
        }",
        vars,
    )
    .await
    .map_err(ServerFnError::new)?;

    Ok(data.live_profile)
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
) -> Result<WithStats<Vec<EventRow>>, ServerFnError> {
    use crate::server::AppState;
    use crate::server::query_api::graphql_query_with_stats;

    let state = expect_context::<AppState>();

    #[derive(Deserialize)]
    struct Response {
        events: Vec<EventRow>,
    }

    let vars = serde_json::json!({
        "eventType": event_type,
        "deviceType": device_type,
    });
    let (data, stats): (Response, Vec<crate::server::QueryStatEntry>) = graphql_query_with_stats(
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

    Ok(WithStats { data: data.events, stats })
}

#[server(GetEvent)]
pub async fn get_event(tenant_id: String, event_id: String, event_date_hint: String) -> Result<EventRow, ServerFnError> {
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
        "eventDateHint": event_date_hint,
    });
    let data: Response = graphql_query(
        &state,
        "query($tenantId: String!, $eventId: String!, $eventDateHint: String) { \
            event(tenantId: $tenantId, eventId: $eventId, eventDateHint: $eventDateHint) { \
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

#[cfg(test)]
mod release_profile_tests {
    use super::LiveProfile;

    #[test]
    fn profile_retains_nullable_fractional_completed_average_without_deferred_fields() {
        for (value, expected) in [(serde_json::Value::Null, None), (serde_json::json!(2.125), Some(2.125))] {
            let row = serde_json::json!({
                "canonicalId":"person", "avgSessionDurationSec":value,
                "topPages":["/home"], "topFeatures":[]
            });
            let profile: LiveProfile = serde_json::from_value(row).unwrap();
            assert_eq!(profile.avg_session_duration_sec, expected);
            let serialized = serde_json::to_value(profile).unwrap();
            for field in ["total_sessions", "sessions_1d", "sessions_7d", "sessions_30d", "sessions_90d", "current_session_active", "current_session_duration_sec"] {
                assert!(serialized.get(field).is_none(), "deferred field: {field}");
            }
        }
    }
}
