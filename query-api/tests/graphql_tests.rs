use async_graphql::{EmptyMutation, Schema};
use async_trait::async_trait;
use query_api::db::pinot::{
    parse_jsonl, sanitize_input, sanitize_timestamp, PinotQuerier,
};
use query_api::db::LiveProfileProvider;
use query_api::schema::subscription::SubscriptionRoot;
use query_api::schema::QueryRoot;
use query_api::streaming::types::ProfileUpdateMessage;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::broadcast;

struct TestPinot {
    handler: Box<dyn Fn(&str) -> Result<String, String> + Send + Sync>,
}

#[async_trait]
impl PinotQuerier for TestPinot {
    async fn query(&self, sql: &str) -> Result<String, String> {
        (self.handler)(sql)
    }
}

struct TestProfileProvider {
    handler: Box<dyn Fn(&str, &str) -> Result<Option<String>, String> + Send + Sync>,
}

#[async_trait]
impl LiveProfileProvider for TestProfileProvider {
    async fn get_live_profile(
        &self,
        tenant_id: &str,
        canonical_id: &str,
    ) -> Result<Option<String>, String> {
        (self.handler)(tenant_id, canonical_id)
    }
}

fn build_schema(
    pinot: impl Fn(&str) -> Result<String, String> + Send + Sync + 'static,
    profile_provider: impl Fn(&str, &str) -> Result<Option<String>, String> + Send + Sync + 'static,
) -> Schema<QueryRoot, EmptyMutation, SubscriptionRoot> {
    let pinot: Arc<dyn PinotQuerier> = Arc::new(TestPinot {
        handler: Box::new(pinot),
    });
    let profiles: Arc<dyn LiveProfileProvider> = Arc::new(TestProfileProvider {
        handler: Box::new(profile_provider),
    });
    let (tx, _) = broadcast::channel::<ProfileUpdateMessage>(16);

    Schema::build(QueryRoot::default(), EmptyMutation, SubscriptionRoot)
        .data(pinot)
        .data(profiles)
        .data(tx)
        .limit_depth(5)
        .limit_complexity(1000)
        .finish()
}

fn no_profile(_tenant: &str, _id: &str) -> Result<Option<String>, String> {
    Ok(None)
}

// ═══════════════════════════════════════════════════════════════════
// Pure function tests
// ═══════════════════════════════════════════════════════════════════

mod sanitize {
    use super::*;

    #[test]
    fn valid_tenant_id() {
        assert_eq!(sanitize_input("acme-corp").unwrap(), "acme-corp");
    }

    #[test]
    fn valid_with_dots_and_underscores() {
        assert_eq!(
            sanitize_input("tenant_123.prod").unwrap(),
            "tenant_123.prod"
        );
    }

    #[test]
    fn rejects_sql_injection() {
        assert!(sanitize_input("'; DROP TABLE--").is_err());
    }

    #[test]
    fn rejects_single_quote() {
        assert!(sanitize_input("acme'corp").is_err());
    }

    #[test]
    fn rejects_semicolon() {
        assert!(sanitize_input("acme;corp").is_err());
    }

    #[test]
    fn rejects_spaces() {
        assert!(sanitize_input("acme corp").is_err());
    }

    #[test]
    fn rejects_backtick() {
        assert!(sanitize_input("acme`corp").is_err());
    }

    #[test]
    fn empty_string_is_valid() {
        assert_eq!(sanitize_input("").unwrap(), "");
    }
}

mod sanitize_ts {
    use super::*;

    #[test]
    fn valid_iso_date() {
        assert_eq!(
            sanitize_timestamp("2026-05-23").unwrap(),
            "2026-05-23"
        );
    }

    #[test]
    fn valid_datetime_with_t() {
        assert_eq!(
            sanitize_timestamp("2026-05-23T12:30:00").unwrap(),
            "2026-05-23T12:30:00"
        );
    }

    #[test]
    fn valid_datetime_with_z() {
        assert_eq!(
            sanitize_timestamp("2026-05-23T12:30:00Z").unwrap(),
            "2026-05-23T12:30:00Z"
        );
    }

    #[test]
    fn valid_datetime_with_fractional() {
        assert_eq!(
            sanitize_timestamp("2026-05-23T12:30:00.123Z").unwrap(),
            "2026-05-23T12:30:00.123Z"
        );
    }

    #[test]
    fn rejects_too_short() {
        assert!(sanitize_timestamp("2026").is_err());
    }

    #[test]
    fn rejects_sql_injection_in_timestamp() {
        assert!(sanitize_timestamp("2026'; DROP--").is_err());
    }

    #[test]
    fn rejects_letters_in_timestamp() {
        assert!(sanitize_timestamp("2026-05-23Thello").is_err());
    }
}

mod parse {
    use super::*;

    #[derive(Deserialize, Debug, PartialEq)]
    struct Row {
        id: u64,
        name: String,
    }

    #[test]
    fn parses_multiple_lines() {
        let input = r#"{"id":1,"name":"alice"}
{"id":2,"name":"bob"}"#;
        let rows: Vec<Row> = parse_jsonl(input);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].id, 1);
        assert_eq!(rows[1].name, "bob");
    }

    #[test]
    fn skips_empty_lines() {
        let input = "\n{\"id\":1,\"name\":\"a\"}\n\n";
        let rows: Vec<Row> = parse_jsonl(input);
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn skips_invalid_json() {
        let input = "{\"id\":1,\"name\":\"a\"}\nnot json\n{\"id\":2,\"name\":\"b\"}";
        let rows: Vec<Row> = parse_jsonl(input);
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn empty_input_returns_empty() {
        let rows: Vec<Row> = parse_jsonl("");
        assert!(rows.is_empty());
    }
}

// ═══════════════════════════════════════════════════════════════════
// Enum conversion tests
// ═══════════════════════════════════════════════════════════════════

mod enums {
    use query_api::schema::aggregation::*;

    #[test]
    fn dimension_to_column_mappings() {
        assert_eq!(Dimension::Country.to_column(), "country");
        assert_eq!(Dimension::DeviceType.to_column(), "device_type");
        assert_eq!(Dimension::Browser.to_column(), "browser");
        assert_eq!(Dimension::Os.to_column(), "os");
        assert_eq!(Dimension::EventType.to_column(), "event_type");
        assert_eq!(Dimension::PageUrl.to_column(), "page_url");
    }

    #[test]
    fn metric_to_sql_mappings() {
        assert_eq!(Metric::Count.to_sql(), "COUNT(*)");
        assert_eq!(Metric::UniqueSessions.to_sql(), "DISTINCTCOUNTHLL(session_id)");
    }

    #[test]
    fn sort_to_column_mappings() {
        assert_eq!(UserSort::FirstSeen.to_column(), "first_seen");
        assert_eq!(UserSort::LastSeen.to_column(), "last_seen");
        assert_eq!(UserSort::TotalEvents.to_column(), "total_events");
        assert_eq!(UserSort::TotalSessions.to_column(), "total_sessions");
    }

    #[test]
    fn sort_order_to_sql() {
        assert_eq!(SortOrder::Asc.to_sql(), "ASC");
        assert_eq!(SortOrder::Desc.to_sql(), "DESC");
    }
}

// ═══════════════════════════════════════════════════════════════════
// GraphQL schema tests
// ═══════════════════════════════════════════════════════════════════

mod schema_tests {
    use super::*;

    #[tokio::test]
    async fn query_tenants() {
        let schema = build_schema(
            |sql| {
                if sql.contains("FROM profiles") {
                    return Ok(r#"{"tenant_id":"acme-corp","unique_users":42}
{"tenant_id":"widgets-inc","unique_users":15}"#
                        .into());
                }
                assert!(sql.contains("FROM events"));
                assert!(sql.contains("GROUP BY tenant_id"));
                Ok(r#"{"tenant_id":"acme-corp","total_events":1500}
{"tenant_id":"widgets-inc","total_events":800}"#
                    .into())
            },
            no_profile,
        );

        let resp = schema
            .execute("{ tenants { id totalEvents uniqueUsers } }")
            .await;

        assert!(resp.errors.is_empty(), "errors: {:?}", resp.errors);
        let data = resp.data.into_json().unwrap();
        let tenants = &data["tenants"];
        assert_eq!(tenants.as_array().unwrap().len(), 2);
        assert_eq!(tenants[0]["id"], "acme-corp");
        assert_eq!(tenants[0]["totalEvents"], 1500);
        assert_eq!(tenants[0]["uniqueUsers"], 42);
        assert_eq!(tenants[1]["id"], "widgets-inc");
    }

    #[tokio::test]
    async fn query_single_tenant() {
        let schema = build_schema(
            |sql| {
                assert!(sql.contains("tenant_id = 'acme-corp'"));
                if sql.contains("unique_users") {
                    return Ok(r#"{"unique_users":42}"#.into());
                }
                Ok(r#"{"tenant_id":"acme-corp","total_events":1500}"#.into())
            },
            no_profile,
        );

        let resp = schema
            .execute(r#"{ tenant(id: "acme-corp") { id totalEvents } }"#)
            .await;

        assert!(resp.errors.is_empty(), "errors: {:?}", resp.errors);
        let data = resp.data.into_json().unwrap();
        assert_eq!(data["tenant"]["id"], "acme-corp");
        assert_eq!(data["tenant"]["totalEvents"], 1500);
    }

    #[tokio::test]
    async fn query_tenant_not_found() {
        let schema = build_schema(|_sql| Ok("".into()), no_profile);

        let resp = schema
            .execute(r#"{ tenant(id: "nonexistent") { id } }"#)
            .await;

        assert!(resp.errors.is_empty());
        let data = resp.data.into_json().unwrap();
        assert!(data["tenant"].is_null());
    }

    #[tokio::test]
    async fn query_tenant_events() {
        let schema = build_schema(
            |sql| {
                if sql.contains("unique_users") {
                    return Ok(r#"{"unique_users":10}"#.into());
                }
                if sql.contains("GROUP BY tenant_id") {
                    return Ok(
                        r#"{"tenant_id":"acme-corp","total_events":100}"#.into(),
                    );
                }
                assert!(sql.contains("tenant_id = 'acme-corp'"));
                assert!(sql.contains("ORDER BY event_time DESC"));
                assert!(sql.contains("LIMIT 2"));
                Ok(r#"{"event_id":"e1","event_type":"page_view","tenant_id":"acme-corp","event_time":"2026-05-23 12:00:00Z","canonical_id":"u1","anonymous_id":"a1","user_id":"alice","page_url":"/home","device_type":"desktop","browser":"Chrome","country":"US"}
{"event_id":"e2","event_type":"click","tenant_id":"acme-corp","event_time":"2026-05-23 12:01:00Z","canonical_id":"u1","anonymous_id":"a1","user_id":"alice","page_url":"/about","device_type":"desktop","browser":"Chrome","country":"US"}"#.into())
            },
            no_profile,
        );

        let resp = schema
            .execute(
                r#"{ tenant(id: "acme-corp") { events(limit: 2) { eventId eventType pageUrl } } }"#,
            )
            .await;

        assert!(resp.errors.is_empty(), "errors: {:?}", resp.errors);
        let data = resp.data.into_json().unwrap();
        let events = &data["tenant"]["events"];
        assert_eq!(events.as_array().unwrap().len(), 2);
        assert_eq!(events[0]["eventType"], "page_view");
        assert_eq!(events[1]["eventType"], "click");
    }

    #[tokio::test]
    async fn query_tenant_events_with_filters() {
        let schema = build_schema(
            |sql| {
                if sql.contains("unique_users") {
                    return Ok(r#"{"unique_users":10}"#.into());
                }
                if sql.contains("GROUP BY tenant_id") {
                    return Ok(
                        r#"{"tenant_id":"acme-corp","total_events":100}"#.into(),
                    );
                }
                assert!(
                    sql.contains("event_type = 'page_view'"),
                    "expected event_type filter, got: {sql}"
                );
                assert!(
                    sql.contains("canonical_id = 'user-123'"),
                    "expected canonical_id filter, got: {sql}"
                );
                Ok("".into())
            },
            no_profile,
        );

        let resp = schema
            .execute(
                r#"{ tenant(id: "acme-corp") { events(eventType: "page_view", canonicalId: "user-123") { eventId } } }"#,
            )
            .await;

        assert!(resp.errors.is_empty(), "errors: {:?}", resp.errors);
    }

    #[tokio::test]
    async fn query_tenant_events_with_time_range() {
        let schema = build_schema(
            |sql| {
                if sql.contains("unique_users") {
                    return Ok(r#"{"unique_users":10}"#.into());
                }
                if sql.contains("GROUP BY tenant_id") {
                    return Ok(
                        r#"{"tenant_id":"acme-corp","total_events":100}"#.into(),
                    );
                }
                assert!(
                    sql.contains("event_time >= '2026-05-01T00:00:00'"),
                    "expected from filter, got: {sql}"
                );
                assert!(
                    sql.contains("event_time <= '2026-05-23T23:59:59'"),
                    "expected to filter, got: {sql}"
                );
                Ok("".into())
            },
            no_profile,
        );

        let resp = schema
            .execute(
                r#"{ tenant(id: "acme-corp") { events(from: "2026-05-01T00:00:00", to: "2026-05-23T23:59:59") { eventId } } }"#,
            )
            .await;

        assert!(resp.errors.is_empty(), "errors: {:?}", resp.errors);
    }

    #[tokio::test]
    async fn query_tenant_summary() {
        let schema = build_schema(
            |sql| {
                if sql.contains("unique_users") {
                    return Ok(r#"{"unique_users":10}"#.into());
                }
                if sql.contains("GROUP BY tenant_id") {
                    return Ok(
                        r#"{"tenant_id":"acme-corp","total_events":100}"#.into(),
                    );
                }
                assert!(sql.contains("GROUP BY event_type"));
                Ok(r#"{"event_type":"page_view","total_events":500}
{"event_type":"click","total_events":200}"#
                    .into())
            },
            no_profile,
        );

        let resp = schema
            .execute(r#"{ tenant(id: "acme-corp") { summary { eventType totalEvents } } }"#)
            .await;

        assert!(resp.errors.is_empty(), "errors: {:?}", resp.errors);
        let data = resp.data.into_json().unwrap();
        let summary = &data["tenant"]["summary"];
        assert_eq!(summary.as_array().unwrap().len(), 2);
        assert_eq!(summary[0]["eventType"], "page_view");
        assert_eq!(summary[0]["totalEvents"], 500);
    }

    #[tokio::test]
    async fn query_aggregate_by_country() {
        let schema = build_schema(
            |sql| {
                if sql.contains("unique_users") {
                    return Ok(r#"{"unique_users":10}"#.into());
                }
                if sql.contains("GROUP BY tenant_id") {
                    return Ok(
                        r#"{"tenant_id":"acme-corp","total_events":100}"#.into(),
                    );
                }
                assert!(
                    sql.contains("country as key"),
                    "expected country dimension, got: {sql}"
                );
                assert!(
                    sql.contains("COUNT(*) as count"),
                    "expected count metric, got: {sql}"
                );
                Ok(r#"{"key":"US","count":500}
{"key":"GB","count":200}"#
                    .into())
            },
            no_profile,
        );

        let resp = schema
            .execute(
                r#"{ tenant(id: "acme-corp") { aggregate(groupBy: COUNTRY) { key count } } }"#,
            )
            .await;

        assert!(resp.errors.is_empty(), "errors: {:?}", resp.errors);
        let data = resp.data.into_json().unwrap();
        let agg = &data["tenant"]["aggregate"];
        assert_eq!(agg[0]["key"], "US");
        assert_eq!(agg[0]["count"], 500);
    }

    #[tokio::test]
    async fn query_aggregate_unique_sessions_by_device() {
        let schema = build_schema(
            |sql| {
                if sql.contains("unique_users") {
                    return Ok(r#"{"unique_users":10}"#.into());
                }
                if sql.contains("GROUP BY tenant_id") {
                    return Ok(
                        r#"{"tenant_id":"acme-corp","total_events":100}"#.into(),
                    );
                }
                assert!(
                    sql.contains("device_type as key"),
                    "expected device_type dimension, got: {sql}"
                );
                assert!(
                    sql.contains("DISTINCTCOUNTHLL(session_id) as count"),
                    "expected unique_sessions metric, got: {sql}"
                );
                Ok(r#"{"key":"desktop","count":30}
{"key":"mobile","count":12}"#
                    .into())
            },
            no_profile,
        );

        let resp = schema
            .execute(
                r#"{ tenant(id: "acme-corp") { aggregate(groupBy: DEVICE_TYPE, metric: UNIQUE_SESSIONS) { key count } } }"#,
            )
            .await;

        assert!(resp.errors.is_empty(), "errors: {:?}", resp.errors);
        let data = resp.data.into_json().unwrap();
        assert_eq!(data["tenant"]["aggregate"][0]["key"], "desktop");
    }

    #[tokio::test]
    async fn query_multiple_aggregations_with_aliases() {
        let schema = build_schema(
            |sql| {
                if sql.contains("unique_users") {
                    return Ok(r#"{"unique_users":10}"#.into());
                }
                if sql.contains("GROUP BY tenant_id") {
                    return Ok(
                        r#"{"tenant_id":"acme-corp","total_events":100}"#.into(),
                    );
                }
                if sql.contains("country as key") {
                    return Ok(r#"{"key":"US","count":500}"#.into());
                }
                if sql.contains("device_type as key") {
                    return Ok(r#"{"key":"desktop","count":30}"#.into());
                }
                Ok("".into())
            },
            no_profile,
        );

        let resp = schema
            .execute(
                r#"{
                    tenant(id: "acme-corp") {
                        byCountry: aggregate(groupBy: COUNTRY) { key count }
                        byDevice: aggregate(groupBy: DEVICE_TYPE) { key count }
                    }
                }"#,
            )
            .await;

        assert!(resp.errors.is_empty(), "errors: {:?}", resp.errors);
        let data = resp.data.into_json().unwrap();
        assert_eq!(data["tenant"]["byCountry"][0]["key"], "US");
        assert_eq!(data["tenant"]["byDevice"][0]["key"], "desktop");
    }

    #[tokio::test]
    async fn query_users_with_pagination() {
        let schema = build_schema(
            |sql| {
                if sql.contains("unique_users") {
                    return Ok(r#"{"unique_users":10}"#.into());
                }
                if sql.contains("GROUP BY tenant_id") {
                    return Ok(
                        r#"{"tenant_id":"acme-corp","total_events":100}"#.into(),
                    );
                }
                if sql.contains("COUNT(*) AS total") {
                    return Ok(r#"{"total":42}"#.into());
                }
                assert!(
                    sql.contains("LIMIT 10 OFFSET 20"),
                    "expected pagination, got: {sql}"
                );
                assert!(
                    sql.contains("ORDER BY first_seen ASC"),
                    "expected sort, got: {sql}"
                );
                Ok(r#"{"tenant_id":"acme-corp","canonical_id":"u1","first_seen":"2026-01-01 00:00:00Z","last_seen":"2026-05-23 12:00:00Z","total_events":50,"total_sessions":5,"page_views":30,"clicks":10,"signups":1,"logins":3,"feature_uses":6,"last_country":"US","last_device":"desktop","last_browser":"Chrome"}"#.into())
            },
            no_profile,
        );

        let resp = schema
            .execute(
                r#"{ tenant(id: "acme-corp") { users(limit: 10, offset: 20, sort: FIRST_SEEN, order: ASC) { totalCount nodes { canonicalId totalEvents lastCountry } } } }"#,
            )
            .await;

        assert!(resp.errors.is_empty(), "errors: {:?}", resp.errors);
        let data = resp.data.into_json().unwrap();
        assert_eq!(data["tenant"]["users"]["totalCount"], 42);
        assert_eq!(data["tenant"]["users"]["nodes"][0]["canonicalId"], "u1");
        assert_eq!(data["tenant"]["users"]["nodes"][0]["totalEvents"], 50);
    }

    #[tokio::test]
    async fn query_user_live_profile() {
        let schema = build_schema(
            |sql| {
                if sql.contains("unique_users") {
                    return Ok(r#"{"unique_users":10}"#.into());
                }
                if sql.contains("GROUP BY tenant_id") {
                    return Ok(
                        r#"{"tenant_id":"acme-corp","total_events":100}"#.into(),
                    );
                }
                if sql.contains("COUNT(*) AS total") {
                    return Ok(r#"{"total":1}"#.into());
                }
                Ok(r#"{"tenant_id":"acme-corp","canonical_id":"u1","first_seen":"2026-01-01 00:00:00Z","last_seen":"2026-05-23 12:00:00Z","total_events":50,"total_sessions":5,"page_views":30,"clicks":10,"signups":1,"logins":3,"feature_uses":6,"last_country":"US","last_device":"desktop","last_browser":"Chrome"}"#.into())
            },
            |tenant, id| {
                assert_eq!(tenant, "acme-corp");
                assert_eq!(id, "u1");
                Ok(Some(
                    r#"{"canonical_id":"u1","user_id":"alice","total_events":50,"total_sessions":5,"events_1d":3,"events_7d":20,"events_30d":40,"events_90d":50,"sessions_1d":1,"sessions_7d":3,"avg_session_duration_sec":120,"current_session_active":true,"current_session_duration_sec":300,"page_views":30,"clicks":10,"logins":3,"feature_uses":6,"last_page":"/dashboard","last_country":"US","last_device":"desktop","last_browser":"Chrome","top_pages":["/home","/settings"],"top_features":["search","export"]}"#.into(),
                ))
            },
        );

        let resp = schema
            .execute(
                r#"{ tenant(id: "acme-corp") { users(limit: 1) { nodes { canonicalId liveProfile { userId currentSessionActive topPages topFeatures } } } } }"#,
            )
            .await;

        assert!(resp.errors.is_empty(), "errors: {:?}", resp.errors);
        let data = resp.data.into_json().unwrap();
        let profile = &data["tenant"]["users"]["nodes"][0]["liveProfile"];
        assert_eq!(profile["userId"], "alice");
        assert_eq!(profile["currentSessionActive"], true);
        assert_eq!(profile["topPages"][0], "/home");
        assert_eq!(profile["topFeatures"][0], "search");
    }

    #[tokio::test]
    async fn query_user_live_profile_not_found() {
        let schema = build_schema(
            |sql| {
                if sql.contains("unique_users") {
                    return Ok(r#"{"unique_users":10}"#.into());
                }
                if sql.contains("GROUP BY tenant_id") {
                    return Ok(
                        r#"{"tenant_id":"acme-corp","total_events":100}"#.into(),
                    );
                }
                if sql.contains("COUNT(*) AS total") {
                    return Ok(r#"{"total":1}"#.into());
                }
                Ok(r#"{"tenant_id":"acme-corp","canonical_id":"u1","first_seen":"2026-01-01 00:00:00Z","last_seen":"2026-05-23 12:00:00Z","total_events":50,"total_sessions":5,"page_views":30,"clicks":10,"signups":1,"logins":3,"feature_uses":6,"last_country":"US","last_device":"desktop","last_browser":"Chrome"}"#.into())
            },
            no_profile,
        );

        let resp = schema
            .execute(
                r#"{ tenant(id: "acme-corp") { users(limit: 1) { nodes { canonicalId liveProfile { userId } } } } }"#,
            )
            .await;

        assert!(resp.errors.is_empty(), "errors: {:?}", resp.errors);
        let data = resp.data.into_json().unwrap();
        assert!(data["tenant"]["users"]["nodes"][0]["liveProfile"].is_null());
    }

    #[tokio::test]
    async fn query_user_recent_events() {
        let schema = build_schema(
            |sql| {
                if sql.contains("unique_users") {
                    return Ok(r#"{"unique_users":10}"#.into());
                }
                if sql.contains("GROUP BY tenant_id") {
                    return Ok(
                        r#"{"tenant_id":"acme-corp","total_events":100}"#.into(),
                    );
                }
                if sql.contains("COUNT(*) AS total") {
                    return Ok(r#"{"total":1}"#.into());
                }
                if sql.contains("FROM profiles") {
                    return Ok(r#"{"tenant_id":"acme-corp","canonical_id":"u1","first_seen":"2026-01-01 00:00:00Z","last_seen":"2026-05-23 12:00:00Z","total_events":50,"total_sessions":5,"page_views":30,"clicks":10,"signups":1,"logins":3,"feature_uses":6,"last_country":"US","last_device":"desktop","last_browser":"Chrome"}"#.into());
                }
                assert!(
                    sql.contains("canonical_id = 'u1'"),
                    "expected canonical_id filter, got: {sql}"
                );
                assert!(
                    sql.contains("LIMIT 5"),
                    "expected limit, got: {sql}"
                );
                Ok(r#"{"event_id":"e1","event_type":"page_view","tenant_id":"acme-corp","event_time":"2026-05-23 12:00:00Z","canonical_id":"u1","anonymous_id":"a1","user_id":"alice","page_url":"/home","device_type":"desktop","browser":"Chrome","country":"US"}"#.into())
            },
            no_profile,
        );

        let resp = schema
            .execute(
                r#"{ tenant(id: "acme-corp") { users(limit: 1) { nodes { canonicalId recentEvents(limit: 5) { eventType pageUrl } } } } }"#,
            )
            .await;

        assert!(resp.errors.is_empty(), "errors: {:?}", resp.errors);
        let data = resp.data.into_json().unwrap();
        let events = &data["tenant"]["users"]["nodes"][0]["recentEvents"];
        assert_eq!(events[0]["eventType"], "page_view");
        assert_eq!(events[0]["pageUrl"], "/home");
    }

    #[tokio::test]
    async fn query_tenant_active_sessions() {
        let schema = build_schema(
            |sql| {
                if sql.contains("unique_users") {
                    return Ok(r#"{"unique_users":10}"#.into());
                }
                if sql.contains("GROUP BY tenant_id") {
                    return Ok(
                        r#"{"tenant_id":"acme-corp","total_events":100}"#.into(),
                    );
                }
                if sql.contains("DISTINCTCOUNTHLL(session_id)") {
                    assert!(
                        sql.contains("tenant_id = 'acme-corp'"),
                        "expected tenant filter, got: {sql}"
                    );
                    assert!(
                        sql.contains("event_time >"),
                        "expected time filter, got: {sql}"
                    );
                    return Ok(r#"{"active_sessions":7}"#.into());
                }
                Ok("".into())
            },
            no_profile,
        );

        let resp = schema
            .execute(r#"{ tenant(id: "acme-corp") { id activeSessions } }"#)
            .await;

        assert!(resp.errors.is_empty(), "errors: {:?}", resp.errors);
        let data = resp.data.into_json().unwrap();
        assert_eq!(data["tenant"]["id"], "acme-corp");
        assert_eq!(data["tenant"]["activeSessions"], 7);
    }

    #[tokio::test]
    async fn rejects_invalid_tenant_id() {
        let schema = build_schema(|_| panic!("should not reach db"), no_profile);

        let resp = schema
            .execute(r#"{ tenant(id: "'; DROP TABLE--") { id } }"#)
            .await;

        assert!(
            !resp.errors.is_empty(),
            "expected error for SQL injection attempt"
        );
    }

    #[tokio::test]
    async fn pinot_error_propagates() {
        let schema = build_schema(
            |_| Err("Pinot error: connection refused".into()),
            no_profile,
        );

        let resp = schema.execute("{ tenants { id } }").await;

        assert!(!resp.errors.is_empty());
        let err_msg = resp.errors[0].message.to_string();
        assert!(
            err_msg.contains("connection refused"),
            "expected error message, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn query_depth_limit_enforced() {
        let schema = build_schema(|_| Ok("".into()), no_profile);

        let resp = schema
            .execute(
                r#"{
                    tenants {
                        users {
                            nodes {
                                recentEvents {
                                    eventType
                                }
                            }
                        }
                    }
                }"#,
            )
            .await;

        let _ = resp;
    }

    #[tokio::test]
    async fn invalid_query_returns_error() {
        let schema = build_schema(|_| Ok("".into()), no_profile);

        let resp = schema.execute("{ nonexistentField }").await;

        assert!(!resp.errors.is_empty());
    }

    #[tokio::test]
    async fn user_profile_rolling_windows() {
        let schema = build_schema(
            |sql| {
                if sql.contains("unique_users") {
                    return Ok(r#"{"unique_users":10}"#.into());
                }
                if sql.contains("GROUP BY tenant_id") {
                    return Ok(
                        r#"{"tenant_id":"acme-corp","total_events":100}"#.into(),
                    );
                }
                if sql.contains("COUNT(*) AS total") {
                    return Ok(r#"{"total":1}"#.into());
                }
                Ok(r#"{"tenant_id":"acme-corp","canonical_id":"u1","first_seen":"2026-01-01 00:00:00Z","last_seen":"2026-05-23 12:00:00Z","total_events":100,"total_sessions":10,"page_views":60,"clicks":20,"signups":1,"logins":5,"feature_uses":14,"last_country":"US","last_device":"desktop","last_browser":"Chrome","events_1d":5,"events_7d":30,"events_30d":70,"events_90d":100,"sessions_1d":2,"sessions_7d":6,"sessions_30d":8,"sessions_90d":10,"total_closed_sessions":9,"avg_session_duration_sec":180}"#.into())
            },
            no_profile,
        );

        let resp = schema
            .execute(
                r#"{ tenant(id: "acme-corp") { users(limit: 1) { nodes {
                    events1D events7D events30D events90D
                    sessions1D sessions7D sessions30D sessions90D
                    totalClosedSessions avgSessionDurationSec
                } } } }"#,
            )
            .await;

        assert!(resp.errors.is_empty(), "errors: {:?}", resp.errors);
        let data = resp.data.into_json().unwrap();
        let node = &data["tenant"]["users"]["nodes"][0];
        assert_eq!(node["events1D"], 5);
        assert_eq!(node["events7D"], 30);
        assert_eq!(node["events30D"], 70);
        assert_eq!(node["events90D"], 100);
        assert_eq!(node["sessions1D"], 2);
        assert_eq!(node["sessions7D"], 6);
        assert_eq!(node["totalClosedSessions"], 9);
        assert_eq!(node["avgSessionDurationSec"], 180);
    }

    #[tokio::test]
    async fn events_over_time_formats_iso_hour_buckets() {
        let schema = build_schema(
            |sql| {
                if sql.contains("event_hour") {
                    return Ok(r#"{"time_bucket":"2026-05-26 14","event_type":"page_view","value":100}
{"time_bucket":"2026-05-26 15","event_type":"page_view","value":200}"#.into());
                }
                Ok("".into())
            },
            no_profile,
        );

        let resp = schema
            .execute(r#"{ eventsOverTime(range: DAY) { bucket group value } }"#)
            .await;

        assert!(resp.errors.is_empty(), "errors: {:?}", resp.errors);
        let data = resp.data.into_json().unwrap();
        let events = &data["eventsOverTime"];
        assert_eq!(events[0]["bucket"], "14:00");
        assert_eq!(events[1]["bucket"], "15:00");
    }

    #[tokio::test]
    async fn events_over_time_formats_epoch_millis_buckets() {
        // Pinot may return epoch millis when substr transform runs after
        // TIMESTAMP type conversion. 1779804000000 = 2026-05-26 14:00 UTC.
        let schema = build_schema(
            |sql| {
                if sql.contains("event_hour") {
                    return Ok(r#"{"time_bucket":"1779804000000","event_type":"page_view","value":100}
{"time_bucket":"1779807600000","event_type":"page_view","value":200}"#.into());
                }
                Ok("".into())
            },
            no_profile,
        );

        let resp = schema
            .execute(r#"{ eventsOverTime(range: DAY) { bucket group value } }"#)
            .await;

        assert!(resp.errors.is_empty(), "errors: {:?}", resp.errors);
        let data = resp.data.into_json().unwrap();
        let events = &data["eventsOverTime"];
        assert_eq!(events[0]["bucket"], "14:00", "epoch millis should be formatted as HH:00");
        assert_eq!(events[1]["bucket"], "15:00", "epoch millis should be formatted as HH:00");
    }
}
