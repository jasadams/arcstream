use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;

use super::pinot::{PinotQuerier, QueryResult, QueryStats};
use super::LiveProfileProvider;

pub struct FlareDBClient {
    pub url: String,
    pub http: Client,
}

#[derive(Deserialize)]
struct FlareResponse {
    result_table: Option<FlareResultTable>,
    error: Option<String>,
    query_stats: Option<FlareQueryStats>,
}

#[derive(Deserialize)]
struct FlareQueryStats {
    elapsed_ms: u64,
    path: String,
    segments_total: usize,
    segments_indexed: usize,
    rows_scanned: usize,
}

#[derive(Deserialize)]
struct FlareResultTable {
    data_schema: FlareDataSchema,
    rows: Vec<Vec<serde_json::Value>>,
}

#[derive(Deserialize)]
struct FlareDataSchema {
    column_names: Vec<String>,
}

impl FlareDBClient {
    async fn execute_request(&self, sql: &str, include_stats: bool) -> Result<FlareResponse, String> {
        let body = serde_json::json!({"sql": sql});

        let mut req = self.http.post(&self.url);
        if include_stats {
            req = req.header("X-Include-Stats", "true");
        }

        let resp = req
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("FlareDB request failed: {e}"))?;

        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(format!("FlareDB query failed: {text}"));
        }

        let flare_resp: FlareResponse = resp
            .json()
            .await
            .map_err(|e| format!("Failed to parse FlareDB response: {e}"))?;

        if let Some(err) = flare_resp.error {
            return Err(format!("FlareDB query error: {err}"));
        }

        Ok(flare_resp)
    }
}

fn rows_to_jsonl(table: &FlareResultTable) -> String {
    let mut lines = Vec::with_capacity(table.rows.len());
    for row in &table.rows {
        let mut obj = serde_json::Map::new();
        for (i, col_name) in table.data_schema.column_names.iter().enumerate() {
            if let Some(val) = row.get(i) {
                obj.insert(col_name.clone(), val.clone());
            }
        }
        lines.push(serde_json::to_string(&obj).unwrap_or_default());
    }
    lines.join("\n")
}

#[async_trait]
impl PinotQuerier for FlareDBClient {
    async fn query(&self, sql: &str) -> Result<String, String> {
        let resp = self.execute_request(sql, false).await?;
        let table = resp.result_table
            .ok_or_else(|| "FlareDB response missing result_table".to_string())?;
        Ok(rows_to_jsonl(&table))
    }

    async fn query_with_stats(&self, sql: &str) -> Result<QueryResult, String> {
        let resp = self.execute_request(sql, true).await?;

        let stats = resp.query_stats.map(|qs| QueryStats {
            elapsed_ms: Some(qs.elapsed_ms),
            path: Some(qs.path),
            segments_total: Some(qs.segments_total),
            segments_indexed: Some(qs.segments_indexed),
            rows_scanned: Some(qs.rows_scanned),
            backend: "flaredb".to_owned(),
        });

        let table = resp.result_table
            .ok_or_else(|| "FlareDB response missing result_table".to_string())?;

        Ok(QueryResult {
            body: rows_to_jsonl(&table),
            stats,
            sql: sql.to_owned(),
            backend: "flaredb".to_owned(),
        })
    }
}

#[async_trait]
impl LiveProfileProvider for FlareDBClient {
    async fn get_live_profile(
        &self,
        tenant_id: &str,
        canonical_id: &str,
    ) -> Result<Option<String>, String> {
        let sql = format!(
            "SELECT canonical_id, user_id, tenant_id, first_seen, last_seen, \
             total_events, total_sessions, \
             events_1d, events_7d, events_30d, events_90d, \
             sessions_1d, sessions_7d, \
             avg_session_duration_sec, \
             page_views, clicks, logins, feature_uses, \
             last_page, last_country, last_device, last_browser \
             FROM profiles \
             WHERE tenant_id = '{tenant_id}' AND canonical_id = '{canonical_id}' \
             LIMIT 1"
        );

        let resp = self.execute_request(&sql, false).await?;

        let table = match resp.result_table {
            Some(t) => t,
            None => return Ok(None),
        };

        if table.rows.is_empty() {
            return Ok(None);
        }

        let mut obj = serde_json::Map::new();
        for (i, col_name) in table.data_schema.column_names.iter().enumerate() {
            if let Some(val) = table.rows[0].get(i) {
                obj.insert(col_name.clone(), val.clone());
            }
        }

        serde_json::to_string(&obj)
            .map(Some)
            .map_err(|e| format!("JSON serialization failed: {e}"))
    }
}


