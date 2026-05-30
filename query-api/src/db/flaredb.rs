use async_trait::async_trait;
use regex::Regex;
use reqwest::Client;
use serde::Deserialize;
use std::sync::OnceLock;

use super::pinot::{PinotQuerier, QueryResult, QueryStats};

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
        let translated = translate_sql(sql);
        let body = serde_json::json!({"sql": translated});

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

fn ago_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"ago\('(P[^']+)'\)").expect("valid regex"))
}

fn translate_sql(sql: &str) -> String {
    let re = ago_regex();
    re.replace_all(sql, |caps: &regex::Captures| {
        let iso = &caps[1];
        iso_duration_to_sql(iso)
    })
    .into_owned()
}

fn iso_duration_to_sql(iso: &str) -> String {
    if let Some(rest) = iso.strip_prefix("PT") {
        if let Some(minutes) = rest.strip_suffix('M') {
            return format!("CAST((extract(epoch from now()) - {}) * 1000 AS BIGINT)",
                minutes.parse::<u64>().unwrap_or(30) * 60);
        }
        if let Some(hours) = rest.strip_suffix('H') {
            return format!("CAST((extract(epoch from now()) - {}) * 1000 AS BIGINT)",
                hours.parse::<u64>().unwrap_or(1) * 3600);
        }
    }
    if let Some(rest) = iso.strip_prefix('P') {
        if let Some(days) = rest.strip_suffix('D') {
            return format!("CAST((extract(epoch from now()) - {}) * 1000 AS BIGINT)",
                days.parse::<u64>().unwrap_or(1) * 86400);
        }
    }
    format!("0 /* unknown ISO duration: {iso} */")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn translate_ago_30m() {
        let sql = "SELECT COUNT(*) FROM events WHERE event_time > ago('PT30M')";
        let result = translate_sql(sql);
        assert!(result.contains("extract(epoch from now())"));
        assert!(result.contains("1800"));
        assert!(!result.contains("ago"));
    }

    #[test]
    fn translate_ago_1d() {
        let sql = "SELECT COUNT(*) FROM profiles WHERE last_seen > ago('P1D')";
        let result = translate_sql(sql);
        assert!(result.contains("86400"));
        assert!(!result.contains("ago"));
    }

    #[test]
    fn translate_multiple_ago() {
        let sql = "SELECT * FROM events WHERE a > ago('P7D') AND b > ago('P30D')";
        let result = translate_sql(sql);
        assert!(result.contains("604800"));
        assert!(result.contains("2592000"));
        assert!(!result.contains("ago"));
    }

    #[test]
    fn no_ago_unchanged() {
        let sql = "SELECT COUNT(*) FROM events";
        assert_eq!(translate_sql(sql), sql);
    }
}
