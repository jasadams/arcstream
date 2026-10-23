use query_api::streaming::profile_decode::decode;
use serde_json::{json, Value};

fn profile() -> Value {
    json!({
        "canonical_id":"person", "tenant_id":"tenant", "user_id":"user",
        "first_seen":1735689600000u64, "last_seen":1735689605000u64,
        "total_events":2,
        "events_1d":2, "events_7d":2, "events_30d":2, "events_90d":2,

        "avg_session_duration_sec":null,
         "page_views":1, "clicks":1, "logins":0,
        "feature_uses":0, "last_page":"/home", "last_country":"AU",
        "last_device":"desktop", "last_browser":"Firefox",
        "top_pages":["/home"], "top_features":[], "action":"create"
    })
}

fn decode_image(
    row: &Value,
) -> Result<Option<query_api::streaming::types::ProfileUpdateMessage>, String> {
    decode(&serde_json::to_vec(row).unwrap())
}

#[test]
fn native_snapshot_preserves_actual_create_and_update_actions() {
    for action in ["create", "update"] {
        let mut row = profile();
        row["action"] = json!(action);
        let message = decode_image(&row).unwrap().unwrap();
        assert_eq!(message.action, action);
        assert_eq!(message.tenant_id, "tenant");
        assert_eq!(message.profile.total_events, 2);
        assert_eq!(message.profile.first_seen, "2025-01-01 00:00:00.000");
    }
}

#[test]
fn every_release_field_is_required() {
    for field in profile().as_object().unwrap().keys() {
        let mut partial = profile();
        partial.as_object_mut().unwrap().remove(field);
        assert!(
            decode_image(&partial).unwrap_err().contains("22"),
            "accepted missing {field}"
        );
    }
}

#[test]
fn cdc_envelopes_are_rejected_without_an_alternate_format_fallback() {
    for payload in [
        json!({"before":null,"after":profile(),"op":"c"}),
        json!({"before":profile(),"after":profile(),"op":"u"}),
        json!({"before":profile(),"after":null,"op":"d"}),
    ] {
        assert!(decode(&serde_json::to_vec(&payload).unwrap()).is_err());
    }
    assert!(decode(b"null").unwrap().is_none());
}

#[test]
fn malformed_required_types_and_empty_identity_are_rejected() {
    for (field, value) in [
        ("avg_session_duration_sec", json!("2.5")),
        ("total_events", json!("2")),
        ("first_seen", json!("1735689600000")),
        ("top_pages", json!("[\"/home\"]")),
        ("top_features", json!([1])),
        ("tenant_id", json!("")),
        ("canonical_id", json!("")),
    ] {
        let mut row = profile();
        row[field] = value;
        assert!(decode_image(&row).is_err(), "{field}");
    }
}

#[test]
fn historical_profile_timestamps_preserve_pre_epoch_source_time() {
    let mut historical = profile();
    historical["first_seen"] = json!(-1000);
    historical["last_seen"] = json!(-1);
    let message = decode_image(&historical).unwrap().unwrap();
    assert_eq!(message.profile.first_seen, "1969-12-31 23:59:59.000");
    assert_eq!(message.profile.last_seen, "1969-12-31 23:59:59.999");
}

#[test]
fn out_of_range_profile_time_is_rejected_instead_of_blank_timestamp() {
    let mut invalid = profile();
    invalid["first_seen"] = json!(i64::MAX);
    assert!(decode_image(&invalid).is_err());
}

#[test]
fn completed_average_preserves_null_and_fractional_values() {
    for value in [Value::Null, json!(2.125), json!(0.0)] {
        let mut row = profile();
        row["avg_session_duration_sec"] = value.clone();
        let message = decode_image(&row).unwrap().unwrap();
        assert_eq!(
            serde_json::to_value(message.profile.avg_session_duration_sec).unwrap(),
            value
        );
    }
    let mut invalid = profile();
    invalid["avg_session_duration_sec"] = json!(-0.5);
    assert!(decode_image(&invalid).is_err());
}

#[test]
fn deferred_fields_are_rejected_instead_of_silently_filled() {
    for field in [
        "total_sessions",
        "sessions_1d",
        "sessions_7d",
        "sessions_30d",
        "sessions_90d",
        "current_session_active",
        "current_session_duration_sec",
        "updated_at",
        "timestamp",
        "trigger",
        "changed_fields",
    ] {
        let mut row = profile();
        row[field] = json!(0);
        assert!(decode_image(&row).is_err(), "{field}");
    }
}

#[tokio::test]
async fn native_profile_reaches_graphql_with_core_and_completed_average() {
    use async_graphql::{EmptyMutation, Schema};
    use futures_util::StreamExt;
    use query_api::schema::{subscription::SubscriptionRoot, QueryRoot};
    use query_api::streaming::types::ProfileUpdateMessage;
    use tokio::sync::broadcast;

    let mut after = profile();
    after["avg_session_duration_sec"] = json!(2.125);
    let message = decode_image(&after).unwrap().unwrap();
    let (sender, _) = broadcast::channel::<ProfileUpdateMessage>(16);
    let schema = Schema::build(QueryRoot::default(), EmptyMutation, SubscriptionRoot)
        .data(sender.clone())
        .finish();
    let mut stream = Box::pin(schema.execute_stream(
        "subscription { profileUpdates(tenantId: \"tenant\") { tenantId action profile { canonicalId totalEvents events1D events7D events30D events90D avgSessionDurationSec topPages } } }"
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
            "tenantId":"tenant", "action":"create", "profile": {
                "canonicalId":"person", "totalEvents":2,
                "events1D":2, "events7D":2, "events30D":2, "events90D":2, "avgSessionDurationSec":2.125,
                "topPages":["/home"]
            }
        })
    );
}

#[test]
fn unsupported_or_mistyped_native_actions_are_rejected() {
    for action in [
        json!(""),
        json!("timer"),
        json!("insert"),
        json!("delete"),
        json!(null),
        json!(1),
    ] {
        let mut row = profile();
        row["action"] = action;
        assert!(decode_image(&row).is_err());
    }
}
