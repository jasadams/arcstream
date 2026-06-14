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
struct FlareDBResponse {
    result_table: ResultTable,
    #[serde(default)]
    query_stats: Option<FlareDBStats>,
}

#[derive(Deserialize)]
struct ResultTable {
    data_schema: DataSchema,
    rows: Vec<Vec<serde_json::Value>>,
}

#[derive(Deserialize)]
struct DataSchema {
    column_names: Vec<String>,
}

#[derive(Deserialize)]
struct FlareDBStats {
    elapsed_ms: u64,
    path: String,
    segments_total: usize,
    segments_indexed: usize,
    rows_scanned: usize,
}

fn table_to_jsonl(table: &ResultTable) -> String {
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

impl FlareDBClient {
    async fn execute(&self, sql: &str) -> Result<FlareDBResponse, String> {
        let body = serde_json::json!({"sql": sql});

        let resp = self
            .http
            .post(format!("{}/query/sql", self.url))
            .header("X-Include-Stats", "true")
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("FlareDB request failed: {e}"))?;

        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(format!("FlareDB query failed: {text}"));
        }

        resp.json()
            .await
            .map_err(|e| format!("Failed to parse FlareDB response: {e}"))
    }
}

#[async_trait]
impl PinotQuerier for FlareDBClient {
    async fn query(&self, sql: &str) -> Result<String, String> {
        let resp = self.execute(sql).await?;
        Ok(table_to_jsonl(&resp.result_table))
    }

    async fn query_with_stats(&self, sql: &str) -> Result<QueryResult, String> {
        let resp = self.execute(sql).await?;

        let stats = resp.query_stats.map(|s| QueryStats {
            elapsed_ms: Some(s.elapsed_ms),
            path: Some(s.path),
            segments_total: Some(s.segments_total),
            segments_indexed: Some(s.segments_indexed),
            rows_scanned: Some(s.rows_scanned),
            backend: "flaredb".to_owned(),
        });

        Ok(QueryResult {
            body: table_to_jsonl(&resp.result_table),
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

        let resp = self.execute(&sql).await?;

        if resp.result_table.rows.is_empty() {
            return Ok(None);
        }

        let mut obj = serde_json::Map::new();
        for (i, col_name) in resp.result_table.data_schema.column_names.iter().enumerate() {
            if let Some(val) = resp.result_table.rows[0].get(i) {
                obj.insert(col_name.clone(), val.clone());
            }
        }

        serde_json::to_string(&obj)
            .map(Some)
            .map_err(|e| format!("JSON serialization failed: {e}"))
    }
}
