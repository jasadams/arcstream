use async_graphql::{Context, Object};
use serde::Deserialize;
use std::sync::Arc;

use crate::db::pinot::{parse_jsonl, sanitize_input, sanitize_timestamp, PinotQuerier};
use crate::schema::aggregation::{
    Dimension, EventTypeSummary, GroupCount, Metric, SortOrder, UserSort,
};
use crate::schema::event::Event;
use crate::schema::user_profile::{UserConnection, UserProfile, UserProfileRow};

#[derive(Deserialize)]
pub struct TenantRow {
    pub tenant_id: String,
    pub total_events: u64,
    pub unique_users: u64,
}

pub struct Tenant {
    pub id: String,
    pub total_events: u64,
    pub unique_users: u64,
}

#[derive(Deserialize)]
struct ActiveSessionsRow {
    active_sessions: u64,
}

#[Object]
impl Tenant {
    async fn id(&self) -> &str {
        &self.id
    }

    async fn total_events(&self) -> u64 {
        self.total_events
    }

    async fn unique_users(&self) -> u64 {
        self.unique_users
    }

    async fn active_sessions(&self, ctx: &Context<'_>) -> async_graphql::Result<u64> {
        let pinot = ctx.data::<Arc<dyn PinotQuerier>>()?;
        let safe_tenant = sanitize_input(&self.id).map_err(async_graphql::Error::new)?;

        let sql = format!(
            "SELECT DISTINCTCOUNTHLL(session_id) AS active_sessions \
             FROM events \
             WHERE tenant_id = '{safe_tenant}' \
             AND event_time > ago('PT30M')"
        );

        let body = pinot.query(&sql).await.map_err(async_graphql::Error::new)?;
        let rows: Vec<ActiveSessionsRow> = parse_jsonl(&body);
        Ok(rows.first().map(|r| r.active_sessions).unwrap_or(0))
    }

    async fn users(
        &self,
        ctx: &Context<'_>,
        #[graphql(default = 25)] limit: i32,
        #[graphql(default = 0)] offset: i32,
        #[graphql(default_with = "UserSort::LastSeen")] sort: UserSort,
        #[graphql(default_with = "SortOrder::Desc")] order: SortOrder,
    ) -> async_graphql::Result<UserConnection> {
        let pinot = ctx.data::<Arc<dyn PinotQuerier>>()?;
        let safe_tenant = sanitize_input(&self.id).map_err(async_graphql::Error::new)?;
        let limit = limit.clamp(1, 200) as u32;
        let offset = offset.max(0) as u32;
        let sort_col = sort.to_column();
        let order_sql = order.to_sql();

        let data_sql = format!(
            "SELECT \
                tenant_id, canonical_id, \
                first_seen, last_seen, \
                total_events, total_sessions, \
                page_views, clicks, logins, feature_uses, \
                last_country, last_device, last_browser, \
                events_1d, events_7d, events_30d, events_90d, \
                sessions_1d, sessions_7d, sessions_30d, sessions_90d, \
                avg_session_duration_sec \
             FROM profiles \
             WHERE tenant_id = '{safe_tenant}' \
             ORDER BY {sort_col} {order_sql} \
             LIMIT {limit} OFFSET {offset}"
        );
        let count_sql = format!(
            "SELECT COUNT(*) AS total \
             FROM profiles \
             WHERE tenant_id = '{safe_tenant}'"
        );

        let (data_body, count_body) =
            tokio::try_join!(pinot.query(&data_sql), pinot.query(&count_sql),)
                .map_err(async_graphql::Error::new)?;

        let rows: Vec<UserProfileRow> = parse_jsonl(&data_body);
        let nodes: Vec<UserProfile> = rows.into_iter().map(UserProfile::from).collect();

        #[derive(Deserialize)]
        struct CountRow {
            total: u64,
        }
        let count_rows: Vec<CountRow> = parse_jsonl(&count_body);
        let total_count = count_rows.first().map(|r| r.total).unwrap_or(0);

        Ok(UserConnection { nodes, total_count })
    }

    async fn events(
        &self,
        ctx: &Context<'_>,
        #[graphql(default = 50)] limit: i32,
        canonical_id: Option<String>,
        event_type: Option<String>,
        from: Option<String>,
        to: Option<String>,
    ) -> async_graphql::Result<Vec<Event>> {
        let pinot = ctx.data::<Arc<dyn PinotQuerier>>()?;
        let safe_tenant = sanitize_input(&self.id).map_err(async_graphql::Error::new)?;
        let limit = limit.clamp(1, 1000) as u32;

        let mut conditions = vec![format!("tenant_id = '{safe_tenant}'")];

        if let Some(cid) = &canonical_id {
            let safe = sanitize_input(cid).map_err(async_graphql::Error::new)?;
            conditions.push(format!("canonical_id = '{safe}'"));
        }
        if let Some(et) = &event_type {
            let safe = sanitize_input(et).map_err(async_graphql::Error::new)?;
            conditions.push(format!("event_type = '{safe}'"));
        }
        if let Some(f) = &from {
            let safe = sanitize_timestamp(f).map_err(async_graphql::Error::new)?;
            conditions.push(format!("event_time >= '{safe}'"));
        }
        if let Some(t) = &to {
            let safe = sanitize_timestamp(t).map_err(async_graphql::Error::new)?;
            conditions.push(format!("event_time <= '{safe}'"));
        }

        let sql = format!(
            "SELECT event_id, event_type, tenant_id, event_time, canonical_id, \
             anonymous_id, user_id, page_url, device_type, browser, country \
             FROM events \
             WHERE {} ORDER BY event_time DESC LIMIT {limit}",
            conditions.join(" AND ")
        );

        let body = pinot.query(&sql).await.map_err(async_graphql::Error::new)?;
        Ok(parse_jsonl(&body))
    }

    async fn summary(&self, ctx: &Context<'_>) -> async_graphql::Result<Vec<EventTypeSummary>> {
        let pinot = ctx.data::<Arc<dyn PinotQuerier>>()?;
        let safe_tenant = sanitize_input(&self.id).map_err(async_graphql::Error::new)?;

        let sql = format!(
            "SELECT event_type, COUNT(*) as total_events \
             FROM events \
             WHERE tenant_id = '{safe_tenant}' \
             GROUP BY event_type ORDER BY total_events DESC"
        );

        let body = pinot.query(&sql).await.map_err(async_graphql::Error::new)?;
        Ok(parse_jsonl(&body))
    }

    async fn aggregate(
        &self,
        ctx: &Context<'_>,
        group_by: Dimension,
        #[graphql(default_with = "Metric::Count")] metric: Metric,
    ) -> async_graphql::Result<Vec<GroupCount>> {
        let pinot = ctx.data::<Arc<dyn PinotQuerier>>()?;
        let safe_tenant = sanitize_input(&self.id).map_err(async_graphql::Error::new)?;
        let column = group_by.to_column();
        let metric_sql = metric.to_sql();

        let sql = format!(
            "SELECT {column} as key, {metric_sql} as count \
             FROM events \
             WHERE tenant_id = '{safe_tenant}' AND {column} != '' \
             GROUP BY key ORDER BY count DESC LIMIT 100"
        );

        let body = pinot.query(&sql).await.map_err(async_graphql::Error::new)?;
        Ok(parse_jsonl(&body))
    }
}
