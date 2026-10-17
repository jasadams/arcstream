use async_graphql::SimpleObject;
use serde::Deserialize;

#[derive(SimpleObject, Deserialize)]
pub struct Event {
    pub event_id: String,
    pub event_type: String,
    pub tenant_id: String,
    pub event_time: String,
    pub canonical_id: String,
    #[serde(deserialize_with = "crate::optional_text::deserialize")]
    pub anonymous_id: String,
    #[serde(deserialize_with = "crate::optional_text::deserialize")]
    pub user_id: String,
    #[serde(deserialize_with = "crate::optional_text::deserialize")]
    pub page_url: String,
    #[serde(deserialize_with = "crate::optional_text::deserialize")]
    pub device_type: String,
    #[serde(deserialize_with = "crate::optional_text::deserialize")]
    pub browser: String,
    #[serde(deserialize_with = "crate::optional_text::deserialize")]
    pub country: String,
}
