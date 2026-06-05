#[cfg(feature = "ssr")]
pub mod query_api;
pub mod api;
pub mod stats_api;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct QueryStatEntry {
    pub sql: String,
    pub backend: String,
    pub elapsed_ms: Option<u64>,
    pub path: Option<String>,
    pub segments_total: Option<usize>,
    pub segments_indexed: Option<usize>,
    pub rows_scanned: Option<usize>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct WithStats<T: Clone> {
    pub data: T,
    pub stats: Vec<QueryStatEntry>,
}

#[cfg(feature = "ssr")]
#[derive(Clone)]
pub struct AppState {
    pub query_api_url: String,
    pub http: reqwest::Client,
}
