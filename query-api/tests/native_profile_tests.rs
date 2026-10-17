use query_api::streaming::profile_decode::decode;
use serde_json::{json, Value};

fn profile() -> Value {
    json!({
        "canonical_id":"person", "tenant_id":"tenant", "user_id":"user",
        "first_seen":1735689600000u64, "last_seen":1735689605000u64,
        "updated_at":1735689605000u64, "total_events":2, "total_sessions":1,
        "events_1d":2, "events_7d":2, "events_30d":2, "events_90d":2,
        "sessions_1d":1, "sessions_7d":1, "sessions_30d":1, "sessions_90d":1,
        "avg_session_duration_sec":0, "current_session_active":true,
        "current_session_duration_sec":5, "page_views":1, "clicks":1, "logins":0,
        "feature_uses":0, "last_page":"/home", "last_country":"AU",
        "last_device":"desktop", "last_browser":"Firefox",
        "top_pages":["/home"], "top_features":[], "action":"update",
        "changed_fields":["total_events","clicks"],
        "timestamp":"2025-01-01 00:00:05.000", "trigger":"event"
    })
}

#[test]
fn native_update_publishes_after_image_with_real_session_fields() {
    let mut before = profile();
    before["total_events"] = json!(1);
    before["current_session_duration_sec"] = json!(0);
    let payload = json!({"before":before,"after":profile(),"op":"u"});
    let message = decode(&serde_json::to_vec(&payload).unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(message.tenant_id, "tenant");
    assert_eq!(message.profile.total_events, 2);
    assert_eq!(message.profile.sessions_30d, 1);
    assert_eq!(message.profile.sessions_90d, 1);
    assert!(message.profile.current_session_active);
    assert_eq!(message.profile.current_session_duration_sec, 5);
    assert_eq!(message.profile.first_seen, "2025-01-01 00:00:00.000");
    assert_eq!(message.changed_fields, vec!["total_events", "clicks"]);
    assert_eq!(message.trigger, "event");
}

#[test]
fn native_create_and_snapshot_require_complete_application_schema() {
    for op in ["c", "r"] {
        let payload = json!({"before":null,"after":profile(),"op":op});
        assert!(decode(&serde_json::to_vec(&payload).unwrap())
            .unwrap()
            .is_some());
    }
    let mut partial = profile();
    for field in [
        "updated_at",
        "avg_session_duration_sec",
        "current_session_active",
        "current_session_duration_sec",
        "action",
        "changed_fields",
        "timestamp",
        "trigger",
    ] {
        partial.as_object_mut().unwrap().remove(field);
    }
    let payload = json!({"before":null,"after":partial,"op":"c"});
    assert!(decode(&serde_json::to_vec(&payload).unwrap())
        .unwrap_err()
        .contains("33"));
}

#[test]
fn native_cdc_rejects_cross_tenant_and_malformed_retractions() {
    let mut other = profile();
    other["tenant_id"] = json!("other-tenant");
    for payload in [
        json!({"before":other,"after":profile(),"op":"u"}),
        json!({"before":null,"after":profile(),"op":"u"}),
        json!({"before":profile(),"after":null,"op":"d"}),
        json!({"before":null,"after":profile(),"op":"unknown"}),
    ] {
        assert!(decode(&serde_json::to_vec(&payload).unwrap()).is_err());
    }
    let mut wrong_type = profile();
    wrong_type["current_session_active"] = json!("true");
    let payload = json!({"before":null,"after":wrong_type,"op":"c"});
    assert!(decode(&serde_json::to_vec(&payload).unwrap()).is_err());
}

#[test]
fn flat_application_profile_and_kafka_tombstones_are_supported() {
    let message = decode(&serde_json::to_vec(&profile()).unwrap())
        .unwrap()
        .unwrap();
    assert!(message.profile.current_session_active);
    assert_eq!(message.profile.current_session_duration_sec, 5);
    assert!(decode(b"null").unwrap().is_none());
}

#[test]
fn historical_profile_timestamps_preserve_pre_epoch_source_time() {
    let mut historical = profile();
    historical["first_seen"] = json!(-1000);
    historical["last_seen"] = json!(-1);
    let message = decode(&serde_json::to_vec(&historical).unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(message.profile.first_seen, "1969-12-31 23:59:59.000");
    assert_eq!(message.profile.last_seen, "1969-12-31 23:59:59.999");
}

#[test]
fn out_of_range_profile_time_is_rejected_instead_of_blank_timestamp() {
    let mut invalid = profile();
    invalid["first_seen"] = json!(i64::MAX);
    assert!(decode(&serde_json::to_vec(&invalid).unwrap()).is_err());
}

#[test]
fn live_session_duration_retains_signed_clock_skew() {
    let mut future_session = profile();
    future_session["current_session_duration_sec"] = json!(-30);
    let message = decode(&serde_json::to_vec(&future_session).unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(message.profile.current_session_duration_sec, -30);
}

#[tokio::test]
async fn native_profile_reaches_graphql_with_session_state_and_all_window_counters() {
    use async_graphql::{EmptyMutation, Schema};
    use futures_util::StreamExt;
    use query_api::schema::{subscription::SubscriptionRoot, QueryRoot};
    use query_api::streaming::types::ProfileUpdateMessage;
    use tokio::sync::broadcast;

    let mut after = profile();
    after["current_session_duration_sec"] = json!(-30);
    let payload = json!({"before":null,"after":after,"op":"c"});
    let message = decode(&serde_json::to_vec(&payload).unwrap())
        .unwrap()
        .unwrap();
    let (sender, _) = broadcast::channel::<ProfileUpdateMessage>(16);
    let schema = Schema::build(QueryRoot, EmptyMutation, SubscriptionRoot)
        .data(sender.clone())
        .finish();
    let mut stream = Box::pin(schema.execute_stream(
        "subscription { profileUpdates(tenantId: \"tenant\") { tenantId trigger profile { canonicalId totalEvents sessions1D sessions7D sessions30D sessions90D currentSessionActive currentSessionDurationSec topPages } } }"
    ));
    let (response, ()) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        tokio::join!(stream.next(), async {
            while sender.receiver_count() == 0 {
                tokio::task::yield_now().await;
            }
            sender.send(message).unwrap();
        })
    })
    .await
    .expect("profile subscription did not deliver");
    let response = response.expect("profile subscription ended");
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let data = response.data.into_json().unwrap();
    assert_eq!(
        data["profileUpdates"],
        json!({
            "tenantId":"tenant", "trigger":"event", "profile": {
                "canonicalId":"person", "totalEvents":2,
                "sessions1D":1, "sessions7D":1, "sessions30D":1, "sessions90D":1,
                "currentSessionActive":true, "currentSessionDurationSec":-30,
                "topPages":["/home"]
            }
        })
    );
}
