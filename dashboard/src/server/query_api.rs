use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::server::AppState;

#[derive(Serialize)]
struct GraphQLRequest {
    query: &'static str,
    variables: serde_json::Value,
}

#[derive(Deserialize)]
struct GraphQLResponse<T> {
    data: Option<T>,
    errors: Option<Vec<GraphQLError>>,
}

#[derive(Deserialize)]
struct GraphQLError {
    message: String,
}

pub async fn graphql_query<T: DeserializeOwned>(
    state: &AppState,
    query: &'static str,
    variables: serde_json::Value,
) -> Result<T, String> {
    let req = GraphQLRequest { query, variables };

    let backend = state
        .backend
        .read()
        .map(|b| b.clone())
        .unwrap_or_else(|_| "pinot".to_string());

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

    gql_resp
        .data
        .ok_or_else(|| "query-api response missing data".to_string())
}
