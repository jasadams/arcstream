use async_graphql::SimpleObject;
use serde::Deserialize;

#[derive(SimpleObject, Deserialize)]
pub struct Event {
    pub event_id: String,
    pub event_type: String,
    pub tenant_id: String,
    pub event_time: String,
    pub canonical_id: String,
    pub anonymous_id: String,
    pub user_id: String,
    pub page_url: String,
    pub device_type: String,
    pub browser: String,
    pub country: String,
}
