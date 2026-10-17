//! Application Kafka subscription names; defaults preserve the released topics and groups.

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SubscriptionConfig {
    pub(crate) events_topic: String,
    pub(crate) events_group_id: String,
    pub(crate) profiles_topic: String,
    pub(crate) profiles_group_id: String,
}

impl SubscriptionConfig {
    pub(crate) fn from_env() -> Self {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Self {
        Self {
            events_topic: lookup("KAFKA_EVENTS_TOPIC").unwrap_or_else(|| "unified-events".into()),
            events_group_id: lookup("KAFKA_EVENTS_GROUP_ID")
                .unwrap_or_else(|| "query-api-events".into()),
            profiles_topic: lookup("KAFKA_PROFILES_TOPIC")
                .unwrap_or_else(|| "profile-updates".into()),
            profiles_group_id: lookup("KAFKA_PROFILES_GROUP_ID")
                .unwrap_or_else(|| "query-api-subscriptions".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SubscriptionConfig;

    #[test]
    fn missing_configuration_preserves_released_subscription_names() {
        assert_eq!(
            SubscriptionConfig::from_lookup(|_| None),
            SubscriptionConfig {
                events_topic: "unified-events".into(),
                events_group_id: "query-api-events".into(),
                profiles_topic: "profile-updates".into(),
                profiles_group_id: "query-api-subscriptions".into(),
            }
        );
    }

    #[test]
    fn configured_topics_and_groups_are_independent() {
        let config = SubscriptionConfig::from_lookup(|name| match name {
            "KAFKA_EVENTS_TOPIC" => Some("evaluation-events".into()),
            "KAFKA_EVENTS_GROUP_ID" => Some("evaluation-events-viewer".into()),
            "KAFKA_PROFILES_TOPIC" => Some("evaluation-profiles".into()),
            "KAFKA_PROFILES_GROUP_ID" => Some("evaluation-profiles-viewer".into()),
            _ => panic!("unexpected configuration key {name}"),
        });
        assert_eq!(config.events_topic, "evaluation-events");
        assert_eq!(config.events_group_id, "evaluation-events-viewer");
        assert_eq!(config.profiles_topic, "evaluation-profiles");
        assert_eq!(config.profiles_group_id, "evaluation-profiles-viewer");
    }

    #[test]
    fn event_topic_override_preserves_other_defaults() {
        let config = SubscriptionConfig::from_lookup(|name| {
            (name == "KAFKA_EVENTS_TOPIC").then(|| "evaluation-events".into())
        });
        assert_eq!(config.events_topic, "evaluation-events");
        assert_eq!(config.events_group_id, "query-api-events");
        assert_eq!(config.profiles_topic, "profile-updates");
        assert_eq!(config.profiles_group_id, "query-api-subscriptions");
    }
}
