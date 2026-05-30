use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::server::AppState;
use crate::server::QueryStatEntry;

#[derive(Serialize)]
struct GraphQLRequest {
    query: &'static str,
    variables: serde_json::Value,
}

#[derive(Deserialize)]
struct GraphQLResponse<T> {
    data: Option<T>,
    errors: Option<Vec<GraphQLError>>,
    extensions: Option<GraphQLExtensions>,
}

#[derive(Deserialize)]
struct GraphQLExtensions {
    #[serde(default, rename = "queryStats")]
    query_stats: Option<Vec<QueryStatEntry>>,
}

#[derive(Deserialize)]
struct GraphQLError {
    message: String,
}

const VALID_BACKENDS: &[&str] = &["pinot", "flare", "flaredb"];

/// Extract the `dev-backend` cookie value from the current SSR request context.
fn cookie_backend() -> Option<String> {
    use axum::http::request::Parts;
    use leptos::prelude::use_context;

    let parts = use_context::<Parts>()?;
    let cookie_header = parts.headers.get("cookie")?.to_str().ok()?;
    let value = cookie_header
        .split(';')
        .find_map(|c| c.trim().strip_prefix("dev-backend=").map(|v| v.to_owned()))?;

    if VALID_BACKENDS.contains(&value.as_str()) {
        Some(value)
    } else {
        None
    }
}

pub async fn graphql_query_with_stats<T: DeserializeOwned>(
    state: &AppState,
    query: &'static str,
    variables: serde_json::Value,
) -> Result<(T, Vec<QueryStatEntry>), String> {
    let req = GraphQLRequest { query, variables };

    let backend = cookie_backend().unwrap_or_else(|| state.default_backend.clone());

    let resp = state
        .http
        .post(&state.query_api_url)
        .header("X-Backend", &backend)
        .json(&req)
        .send()
        .await
        .map_err(|e| format!("query-api request failed: {e}"))?;

    if !resp.status().is_success() {
        let text = resp.text().await.unwrap_or_default();
        return Err(format!("query-api error: {text}"));
    }

    let gql_resp: GraphQLResponse<T> = resp
        .json()
        .await
        .map_err(|e| format!("Failed to parse query-api response: {e}"))?;

    if let Some(errors) = gql_resp.errors {
        if let Some(first) = errors.first() {
            return Err(format!("GraphQL error: {}", first.message));
        }
    }

    let stats = gql_resp
        .extensions
        .and_then(|ext| ext.query_stats)
        .unwrap_or_default();

    let data = gql_resp
        .data
        .ok_or_else(|| "query-api response missing data".to_string())?;

    Ok((data, stats))
}

pub async fn graphql_query<T: DeserializeOwned>(
    state: &AppState,
    query: &'static str,
    variables: serde_json::Value,
) -> Result<T, String> {
    let (data, _stats) = graphql_query_with_stats(state, query, variables).await?;
    Ok(data)
}
