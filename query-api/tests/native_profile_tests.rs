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
        "top_pages":["/home"], "top_features":[]
    })
}

fn decode_image(
    row: &Value,
) -> Result<Option<query_api::streaming::types::ProfileUpdateMessage>, String> {
    let envelope = json!({"before":null,"after":row,"op":"c"});
    decode(&serde_json::to_vec(&envelope).unwrap())
}

#[test]
fn native_update_publishes_core_after_image() {
    let mut before = profile();
    before["total_events"] = json!(1);
    let payload = json!({"before":before,"after":profile(),"op":"u"});
    let message = decode(&serde_json::to_vec(&payload).unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(message.tenant_id, "tenant");
    assert_eq!(message.profile.total_events, 2);
    assert_eq!(message.action, "update");
    assert_eq!(message.profile.first_seen, "2025-01-01 00:00:00.000");
}

#[test]
fn native_create_and_snapshot_require_complete_application_schema() {
    for op in ["c", "r"] {
        let payload = json!({"before":null,"after":profile(),"op":op});
        assert!(decode(&serde_json::to_vec(&payload).unwrap())
            .unwrap()
            .is_some());
    }
    for field in profile().as_object().unwrap().keys() {
        let mut partial = profile();
        partial.as_object_mut().unwrap().remove(field);
        assert!(
            decode_image(&partial).unwrap_err().contains("21"),
            "accepted missing {field}"
        );
    }
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
    wrong_type["avg_session_duration_sec"] = json!("2.5");
    let payload = json!({"before":null,"after":wrong_type,"op":"c"});
    assert!(decode(&serde_json::to_vec(&payload).unwrap()).is_err());
}

#[test]
fn only_native_cdc_images_and_kafka_tombstones_are_supported() {
    assert!(decode(&serde_json::to_vec(&profile()).unwrap()).is_err());
    assert!(decode(b"null").unwrap().is_none());
    assert_eq!(decode_image(&profile()).unwrap().unwrap().action, "create");
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
    let payload = json!({"before":null,"after":after,"op":"c"});
    let message = decode(&serde_json::to_vec(&payload).unwrap())
        .unwrap()
        .unwrap();
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
fn producer_supplied_action_is_rejected_as_an_unknown_business_field() {
    for action in ["", "timer", "insert", "delete"] {
        let mut row = profile();
        row["action"] = json!(action);
        assert!(decode_image(&row).is_err());
    }
}

#[test]
fn update_validates_before_image_and_does_not_hide_native_deletion() {
    let mut malformed_before = profile();
    malformed_before
        .as_object_mut()
        .unwrap()
        .remove("top_pages");
    let update = json!({"before":malformed_before,"after":profile(),"op":"u"});
    assert!(decode(&serde_json::to_vec(&update).unwrap())
        .unwrap_err()
        .contains("21"));
    let deletion = json!({"before":profile(),"after":null,"op":"d"});
    assert!(decode(&serde_json::to_vec(&deletion).unwrap())
        .unwrap_err()
        .contains("deletion"));
}
