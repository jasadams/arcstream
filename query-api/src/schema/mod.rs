pub mod aggregation;
pub mod event;
pub mod live_profile;
pub mod stats;
pub mod subscription;
pub mod tenant;
pub mod user_profile;

use async_graphql::{Context, EmptyMutation, MergedObject, Object, Schema};
use serde::Deserialize;
use std::sync::Arc;

use crate::db::pinot::{parse_jsonl, sanitize_input, PinotQuerier};
use crate::db::scylla::LiveProfileProvider;
use crate::schema::event::Event;
use crate::schema::live_profile::LiveProfile;
use crate::schema::user_profile::{UserConnection, UserProfile, UserProfileRow};
use stats::StatsQuery;
use subscription::SubscriptionRoot;
use tenant::Tenant;

#[derive(MergedObject, Default)]
pub struct QueryRoot(TenantQuery, StatsQuery, GlobalQuery);

pub type AppSchema = Schema<QueryRoot, EmptyMutation, SubscriptionRoot>;

#[derive(Default)]
pub struct TenantQuery;

#[Object]
impl TenantQuery {
    async fn tenants(&self, ctx: &Context<'_>) -> async_graphql::Result<Vec<Tenant>> {
        let pinot = ctx.data::<Arc<dyn PinotQuerier>>()?;

        let events_sql = "SELECT tenant_id, COUNT(*) as total_events \
                          FROM events GROUP BY tenant_id ORDER BY total_events DESC";
        let users_sql = "SELECT tenant_id, COUNT(*) as unique_users \
                         FROM profiles GROUP BY tenant_id";

        let (events_body, users_body) = tokio::try_join!(
            pinot.query(events_sql),
            pinot.query(users_sql),
        ).map_err(async_graphql::Error::new)?;

        #[derive(Deserialize)]
        struct EventsRow { tenant_id: String, total_events: u64 }
        #[derive(Deserialize)]
        struct UsersRow { tenant_id: String, unique_users: u64 }

        let events: Vec<EventsRow> = parse_jsonl(&events_body);
        let users: Vec<UsersRow> = parse_jsonl(&users_body);
        let user_map: std::collections::HashMap<String, u64> = users.into_iter().map(|r| (r.tenant_id, r.unique_users)).collect();

        Ok(events
            .into_iter()
            .map(|r| Tenant {
                unique_users: user_map.get(&r.tenant_id).copied().unwrap_or(0),
                id: r.tenant_id,
                total_events: r.total_events,
            })
            .collect())
    }

    async fn tenant(
        &self,
        ctx: &Context<'_>,
        id: String,
    ) -> async_graphql::Result<Option<Tenant>> {
        let pinot = ctx.data::<Arc<dyn PinotQuerier>>()?;
        let safe_id = sanitize_input(&id).map_err(async_graphql::Error::new)?;

        let events_sql = format!(
            "SELECT tenant_id, COUNT(*) as total_events \
             FROM events WHERE tenant_id = '{safe_id}' GROUP BY tenant_id"
        );
        let users_sql = format!(
            "SELECT COUNT(*) as unique_users \
             FROM profiles WHERE tenant_id = '{safe_id}'"
        );

        let (events_body, users_body) = tokio::try_join!(
            pinot.query(&events_sql),
            pinot.query(&users_sql),
        ).map_err(async_graphql::Error::new)?;

        #[derive(Deserialize)]
        struct EventsRow { tenant_id: String, total_events: u64 }
        #[derive(Deserialize)]
        struct UsersRow { unique_users: u64 }

        let events: Vec<EventsRow> = parse_jsonl(&events_body);
        let users: Vec<UsersRow> = parse_jsonl(&users_body);

        Ok(events.into_iter().next().map(|r| Tenant {
            id: r.tenant_id,
            total_events: r.total_events,
            unique_users: users.first().map(|u| u.unique_users).unwrap_or(0),
        }))
    }
}

#[derive(Default)]
pub struct GlobalQuery;

const VALID_EVENT_TYPES: &[&str] = &["page_view", "click", "signup", "login", "feature_used"];
const VALID_DEVICE_TYPES: &[&str] = &["desktop", "mobile", "tablet"];

#[Object]
impl GlobalQuery {
    async fn users(
        &self,
        ctx: &Context<'_>,
        #[graphql(default = 25)] limit: i32,
        #[graphql(default = 0)] offset: i32,
    ) -> async_graphql::Result<UserConnection> {
        let pinot = ctx.data::<Arc<dyn PinotQuerier>>()?;
        let limit = limit.clamp(1, 200) as u32;
        let offset = offset.max(0) as u32;

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
             ORDER BY first_seen DESC \
             LIMIT {limit} OFFSET {offset}"
        );
        let count_sql = "SELECT COUNT(*) AS total FROM profiles";

        let (data_body, count_body) =
            tokio::try_join!(pinot.query(&data_sql), pinot.query(count_sql))
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
        #[graphql(default = 100)] limit: i32,
        event_type: Option<String>,
        device_type: Option<String>,
    ) -> async_graphql::Result<Vec<Event>> {
        let pinot = ctx.data::<Arc<dyn PinotQuerier>>()?;
        let limit = limit.clamp(1, 1000) as u32;

        let mut conditions = Vec::new();

        if let Some(ref et) = event_type {
            if !VALID_EVENT_TYPES.contains(&et.as_str()) {
                return Err(async_graphql::Error::new("invalid event_type"));
            }
            conditions.push(format!("event_type = '{et}'"));
        }
        if let Some(ref dt) = device_type {
            if !VALID_DEVICE_TYPES.contains(&dt.as_str()) {
                return Err(async_graphql::Error::new("invalid device_type"));
            }
            conditions.push(format!("device_type = '{dt}'"));
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let sql = format!(
            "SELECT event_id, event_type, tenant_id, event_time, canonical_id, \
             anonymous_id, user_id, page_url, device_type, browser, country \
             FROM events \
             {where_clause} \
             ORDER BY event_time DESC LIMIT {limit}"
        );

        let body = pinot.query(&sql).await.map_err(async_graphql::Error::new)?;
        Ok(parse_jsonl(&body))
    }

    async fn event(
        &self,
        ctx: &Context<'_>,
        tenant_id: String,
        event_id: String,
    ) -> async_graphql::Result<Option<Event>> {
        let pinot = ctx.data::<Arc<dyn PinotQuerier>>()?;
        let safe_tenant = sanitize_input(&tenant_id).map_err(async_graphql::Error::new)?;
        let safe_id = sanitize_input(&event_id).map_err(async_graphql::Error::new)?;

        let sql = format!(
            "SELECT event_id, event_type, tenant_id, event_time, canonical_id, \
             anonymous_id, user_id, page_url, device_type, browser, country \
             FROM events \
             WHERE tenant_id = '{safe_tenant}' AND event_id = '{safe_id}' \
             LIMIT 1"
        );

        let body = pinot.query(&sql).await.map_err(async_graphql::Error::new)?;
        let events: Vec<Event> = parse_jsonl(&body);
        Ok(events.into_iter().next())
    }

    async fn live_profile(
        &self,
        ctx: &Context<'_>,
        tenant_id: String,
        canonical_id: String,
    ) -> async_graphql::Result<Option<LiveProfile>> {
        let safe_tenant = sanitize_input(&tenant_id).map_err(async_graphql::Error::new)?;
        let safe_id = sanitize_input(&canonical_id).map_err(async_graphql::Error::new)?;
        let profile_store = ctx.data::<Arc<dyn LiveProfileProvider>>()?;

        let value = profile_store
            .get_live_profile(&safe_tenant, &safe_id)
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
}
