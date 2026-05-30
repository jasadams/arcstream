#[cfg(feature = "ssr")]
pub mod query_api;
pub mod api;
pub mod stats_api;

#[cfg(feature = "ssr")]
#[derive(Clone)]
pub struct AppState {
    pub query_api_url: String,
    pub http: reqwest::Client,
    pub backend: std::sync::Arc<std::sync::RwLock<String>>,
}
