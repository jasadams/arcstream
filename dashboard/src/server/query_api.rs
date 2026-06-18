use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::server::AppState;
use crate::server::QueryStatEntry;

/// Read the `backend` cookie from the current request, defaulting to "pinot".
/// The dashboard's toggle sets this cookie per-browser; query-api routes on the
/// forwarded `x-backend` header.
async fn read_backend_cookie() -> String {
    leptos_axum::extract::<axum::http::HeaderMap>()
        .await
        .ok()
        .and_then(|h| {
            h.get("cookie")
                .and_then(|c| c.to_str().ok())
                .map(str::to_owned)
        })
        .and_then(|cookies| {
            cookies
                .split(';')
                .find_map(|kv| kv.trim().strip_prefix("backend=").map(str::to_owned))
        })
        .filter(|b| b == "pinot" || b == "flare" || b == "flaredb")
        .unwrap_or_else(|| "pinot".to_owned())
}

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

pub async fn graphql_query_with_stats<T: DeserializeOwned>(
    state: &AppState,
    query: &'static str,
    variables: serde_json::Value,
) -> Result<(T, Vec<QueryStatEntry>), String> {
    let req = GraphQLRequest { query, variables };

    // Per-browser backend selection: read the `backend` cookie from the current
    // request (default Pinot) and forward it to query-api as `x-backend`. Each
    // viewer's browser controls its own backend via the dashboard toggle.
    let backend = read_backend_cookie().await;

    let resp = state
        .http
        .post(&state.query_api_url)
        .header("x-backend", backend)
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
