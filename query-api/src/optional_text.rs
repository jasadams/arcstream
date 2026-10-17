//! Display projection for optional event text. Stored event values remain unchanged.

use serde::{Deserialize, Deserializer};

pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer).map(Option::unwrap_or_default)
}

#[cfg(test)]
mod tests {
    use crate::schema::event::Event;
    use crate::streaming::types::LiveEventMessage;
    use serde_json::{json, Value};

    const OPTIONAL: [&str; 6] = [
        "anonymous_id",
        "user_id",
        "page_url",
        "device_type",
        "browser",
        "country",
    ];
    const REQUIRED: [&str; 5] = [
        "event_id",
        "event_type",
        "tenant_id",
        "event_time",
        "canonical_id",
    ];

    fn event_fields(event: &Event) -> [&str; 6] {
        [
            &event.anonymous_id,
            &event.user_id,
            &event.page_url,
            &event.device_type,
            &event.browser,
            &event.country,
        ]
    }

    fn live_fields(event: &LiveEventMessage) -> [&str; 6] {
        [
            &event.anonymous_id,
            &event.user_id,
            &event.page_url,
            &event.device_type,
            &event.browser,
            &event.country,
        ]
    }

    fn reference_event() -> Value {
        // Original identity-01 reference payload, including nullable user and JSON text properties.
        json!({
            "event_id":"identity-01", "event_type":"page_view", "tenant_id":"tenant-a",
            "event_time":"2026-01-01 00:00:01.000", "canonical_id":"c1",
            "anonymous_id":"device-a", "user_id":null, "session_id":"session-1",
            "page_url":"/page/1", "referrer":"https://example.test/start",
            "element_id":"button-1", "feature_name":"reference-feature",
            "device_type":"desktop", "browser":"Firefox", "os":"Linux", "country":"AU",
            "properties":"{\"step\":1,\"nested\":{\"enabled\":true}}"
        })
    }

    #[test]
    fn actual_reference_null_user_is_visible_without_changing_other_values() {
        let raw = reference_event();
        let event: Event = serde_json::from_value(raw.clone()).unwrap();
        let live: LiveEventMessage = serde_json::from_value(raw.clone()).unwrap();
        assert_eq!(
            event_fields(&event),
            ["device-a", "", "/page/1", "desktop", "Firefox", "AU"]
        );
        assert_eq!(live_fields(&live), event_fields(&event));
        assert_eq!(event.event_id, "identity-01");
        assert_eq!(live.canonical_id, "c1");
        assert!(raw["user_id"].is_null());
        assert_eq!(
            raw["properties"],
            "{\"step\":1,\"nested\":{\"enabled\":true}}"
        );
    }

    #[test]
    fn explicit_null_optional_fields_are_blank_in_both_consumers() {
        let mut raw = reference_event();
        for field in OPTIONAL {
            raw[field] = Value::Null;
        }
        let event: Event = serde_json::from_value(raw.clone()).unwrap();
        let live: LiveEventMessage = serde_json::from_value(raw).unwrap();
        assert_eq!(event_fields(&event), [""; 6]);
        assert_eq!(live_fields(&live), [""; 6]);
    }

    #[test]
    fn missing_optional_contracts_remain_distinct() {
        for field in OPTIONAL {
            let mut raw = reference_event();
            raw.as_object_mut().unwrap().remove(field);
            assert!(
                serde_json::from_value::<Event>(raw.clone()).is_err(),
                "{field}"
            );
            let live: LiveEventMessage = serde_json::from_value(raw).unwrap();
            let index = OPTIONAL.iter().position(|value| *value == field).unwrap();
            assert_eq!(live_fields(&live)[index], "");
        }
    }

    #[test]
    fn empty_and_nonempty_optional_strings_are_preserved_exactly() {
        for text in ["", "  Unicode café / \"quoted\"  "] {
            let mut raw = reference_event();
            for field in OPTIONAL {
                raw[field] = json!(text);
            }
            let event: Event = serde_json::from_value(raw.clone()).unwrap();
            let live: LiveEventMessage = serde_json::from_value(raw).unwrap();
            assert_eq!(event_fields(&event), [text; 6]);
            assert_eq!(live_fields(&live), [text; 6]);
        }
    }

    #[test]
    fn optional_wrong_types_are_rejected_instead_of_hidden_as_blank() {
        for field in OPTIONAL {
            for bad in [json!(42), json!(false), json!([]), json!({"value":"text"})] {
                let mut raw = reference_event();
                raw[field] = bad;
                assert!(
                    serde_json::from_value::<Event>(raw.clone()).is_err(),
                    "{field}"
                );
                assert!(
                    serde_json::from_value::<LiveEventMessage>(raw).is_err(),
                    "{field}"
                );
            }
        }
    }

    #[test]
    fn required_fields_stay_strict_for_missing_null_and_wrong_types() {
        for field in REQUIRED {
            for bad in [None, Some(Value::Null), Some(json!(false)), Some(json!(42))] {
                let mut raw = reference_event();
                if let Some(value) = bad {
                    raw[field] = value;
                } else {
                    raw.as_object_mut().unwrap().remove(field);
                }
                assert!(
                    serde_json::from_value::<Event>(raw.clone()).is_err(),
                    "{field}"
                );
                assert!(
                    serde_json::from_value::<LiveEventMessage>(raw).is_err(),
                    "{field}"
                );
            }
        }
    }

    #[test]
    fn graphql_event_text_fields_remain_nonnullable_strings() {
        let schema = async_graphql::Schema::build(
            crate::schema::QueryRoot::default(),
            async_graphql::EmptyMutation,
            crate::schema::subscription::SubscriptionRoot,
        )
        .finish();
        let sdl = schema.sdl();
        for object in ["Event", "LiveEventMessage"] {
            let body = sdl
                .split(&format!("type {object} {{"))
                .nth(1)
                .unwrap_or_else(|| panic!("missing {object}"))
                .split('}')
                .next()
                .unwrap();
            for field in [
                "eventId",
                "eventType",
                "tenantId",
                "eventTime",
                "canonicalId",
                "anonymousId",
                "userId",
                "pageUrl",
                "deviceType",
                "browser",
                "country",
            ] {
                assert!(
                    body.contains(&format!("{field}: String!")),
                    "{object}.{field}"
                );
            }
        }
    }
}
