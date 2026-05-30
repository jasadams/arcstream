use leptos::prelude::*;
use serde::{Deserialize, Serialize};
use crate::server::WithStats;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum TimeRange {
    Day,
    Week,
    Month,
    Quarter,
}

#[cfg(feature = "ssr")]
impl TimeRange {
    fn to_graphql_enum(&self) -> &'static str {
        match self {
            TimeRange::Day => "DAY",
            TimeRange::Week => "WEEK",
            TimeRange::Month => "MONTH",
            TimeRange::Quarter => "QUARTER",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TimeSeriesPoint {
    pub bucket: String,
    pub value: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GroupedTimeSeriesPoint {
    pub bucket: String,
    pub group: String,
    pub value: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BreakdownRow {
    pub label: String,
    pub value: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnalyticsSummary {
    pub users: u64,
    pub sessions: u64,
    pub events: u64,
    pub avg_duration_sec: f64,
    pub events_per_session: f64,
}

#[server(GetAnalyticsSummary)]
pub async fn get_analytics_summary(range: TimeRange) -> Result<WithStats<AnalyticsSummary>, ServerFnError> {
    use crate::server::AppState;
    use crate::server::query_api::graphql_query_with_stats;

    let state = expect_context::<AppState>();

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Response {
        analytics_summary: AnalyticsSummary,
    }

    let vars = serde_json::json!({ "range": range.to_graphql_enum() });
    let (data, stats): (Response, Vec<crate::server::QueryStatEntry>) = graphql_query_with_stats(
        &state,
        "query($range: TimeRange!) { analyticsSummary(range: $range) { users sessions events avgDurationSec eventsPerSession } }",
        vars,
    )
    .await
    .map_err(ServerFnError::new)?;

    Ok(WithStats { data: data.analytics_summary, stats })
}

#[server(GetEventsOverTime)]
pub async fn get_events_over_time(range: TimeRange) -> Result<WithStats<Vec<GroupedTimeSeriesPoint>>, ServerFnError> {
    use crate::server::AppState;
    use crate::server::query_api::graphql_query_with_stats;

    let state = expect_context::<AppState>();

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Response {
        events_over_time: Vec<GroupedTimeSeriesPoint>,
    }

    let vars = serde_json::json!({ "range": range.to_graphql_enum() });
    let (data, stats): (Response, Vec<crate::server::QueryStatEntry>) = graphql_query_with_stats(
        &state,
        "query($range: TimeRange!) { eventsOverTime(range: $range) { bucket group value } }",
        vars,
    )
    .await
    .map_err(ServerFnError::new)?;

    Ok(WithStats { data: data.events_over_time, stats })
}

#[server(GetUsersOverTime)]
pub async fn get_users_over_time(range: TimeRange) -> Result<WithStats<Vec<TimeSeriesPoint>>, ServerFnError> {
    use crate::server::AppState;
    use crate::server::query_api::graphql_query_with_stats;

    let state = expect_context::<AppState>();

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Response {
        users_over_time: Vec<TimeSeriesPoint>,
    }

    let vars = serde_json::json!({ "range": range.to_graphql_enum() });
    let (data, stats): (Response, Vec<crate::server::QueryStatEntry>) = graphql_query_with_stats(
        &state,
        "query($range: TimeRange!) { usersOverTime(range: $range) { bucket value } }",
        vars,
    )
    .await
    .map_err(ServerFnError::new)?;

    Ok(WithStats { data: data.users_over_time, stats })
}

#[server(GetSessionsOverTime)]
pub async fn get_sessions_over_time(range: TimeRange) -> Result<WithStats<Vec<TimeSeriesPoint>>, ServerFnError> {
    use crate::server::AppState;
    use crate::server::query_api::graphql_query_with_stats;

    let state = expect_context::<AppState>();

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Response {
        sessions_over_time: Vec<TimeSeriesPoint>,
    }

    let vars = serde_json::json!({ "range": range.to_graphql_enum() });
    let (data, stats): (Response, Vec<crate::server::QueryStatEntry>) = graphql_query_with_stats(
        &state,
        "query($range: TimeRange!) { sessionsOverTime(range: $range) { bucket value } }",
        vars,
    )
    .await
    .map_err(ServerFnError::new)?;

    Ok(WithStats { data: data.sessions_over_time, stats })
}

#[server(GetAvgSessionDuration)]
pub async fn get_avg_session_duration(range: TimeRange) -> Result<WithStats<Vec<TimeSeriesPoint>>, ServerFnError> {
    use crate::server::AppState;
    use crate::server::query_api::graphql_query_with_stats;

    let state = expect_context::<AppState>();

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Response {
        avg_session_duration: Vec<TimeSeriesPoint>,
    }

    let vars = serde_json::json!({ "range": range.to_graphql_enum() });
    let (data, stats): (Response, Vec<crate::server::QueryStatEntry>) = graphql_query_with_stats(
        &state,
        "query($range: TimeRange!) { avgSessionDuration(range: $range) { bucket value } }",
        vars,
    )
    .await
    .map_err(ServerFnError::new)?;

    Ok(WithStats { data: data.avg_session_duration, stats })
}

#[server(GetTopPages)]
pub async fn get_top_pages(range: TimeRange) -> Result<WithStats<Vec<BreakdownRow>>, ServerFnError> {
    use crate::server::AppState;
    use crate::server::query_api::graphql_query_with_stats;

    let state = expect_context::<AppState>();

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Response {
        top_pages: Vec<BreakdownRow>,
    }

    let vars = serde_json::json!({ "range": range.to_graphql_enum() });
    let (data, stats): (Response, Vec<crate::server::QueryStatEntry>) = graphql_query_with_stats(
        &state,
        "query($range: TimeRange!) { topPages(range: $range) { label value } }",
        vars,
    )
    .await
    .map_err(ServerFnError::new)?;

    Ok(WithStats { data: data.top_pages, stats })
}

#[server(GetDeviceBreakdown)]
pub async fn get_device_breakdown(range: TimeRange) -> Result<WithStats<Vec<BreakdownRow>>, ServerFnError> {
    use crate::server::AppState;
    use crate::server::query_api::graphql_query_with_stats;

    let state = expect_context::<AppState>();

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Response {
        device_breakdown: Vec<BreakdownRow>,
    }

    let vars = serde_json::json!({ "range": range.to_graphql_enum() });
    let (data, stats): (Response, Vec<crate::server::QueryStatEntry>) = graphql_query_with_stats(
        &state,
        "query($range: TimeRange!) { deviceBreakdown(range: $range) { label value } }",
        vars,
    )
    .await
    .map_err(ServerFnError::new)?;

    Ok(WithStats { data: data.device_breakdown, stats })
}

#[server(GetBrowserBreakdown)]
pub async fn get_browser_breakdown(range: TimeRange) -> Result<WithStats<Vec<BreakdownRow>>, ServerFnError> {
    use crate::server::AppState;
    use crate::server::query_api::graphql_query_with_stats;

    let state = expect_context::<AppState>();

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Response {
        browser_breakdown: Vec<BreakdownRow>,
    }

    let vars = serde_json::json!({ "range": range.to_graphql_enum() });
    let (data, stats): (Response, Vec<crate::server::QueryStatEntry>) = graphql_query_with_stats(
        &state,
        "query($range: TimeRange!) { browserBreakdown(range: $range) { label value } }",
        vars,
    )
    .await
    .map_err(ServerFnError::new)?;

    Ok(WithStats { data: data.browser_breakdown, stats })
}

#[server(GetCountryBreakdown)]
pub async fn get_country_breakdown(range: TimeRange) -> Result<WithStats<Vec<BreakdownRow>>, ServerFnError> {
    use crate::server::AppState;
    use crate::server::query_api::graphql_query_with_stats;

    let state = expect_context::<AppState>();

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Response {
        country_breakdown: Vec<BreakdownRow>,
    }

    let vars = serde_json::json!({ "range": range.to_graphql_enum() });
    let (data, stats): (Response, Vec<crate::server::QueryStatEntry>) = graphql_query_with_stats(
        &state,
        "query($range: TimeRange!) { countryBreakdown(range: $range) { label value } }",
        vars,
    )
    .await
    .map_err(ServerFnError::new)?;

    Ok(WithStats { data: data.country_breakdown, stats })
}
