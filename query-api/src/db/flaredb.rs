use async_trait::async_trait;
use regex::Regex;
use reqwest::Client;
use serde::Deserialize;
use std::sync::OnceLock;

use super::pinot::PinotQuerier;

pub struct FlareDBClient {
    pub url: String,
    pub http: Client,
}

#[derive(Deserialize)]
struct FlareResponse {
    result_table: Option<FlareResultTable>,
    error: Option<String>,
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

#[async_trait]
impl PinotQuerier for FlareDBClient {
    async fn query(&self, sql: &str) -> Result<String, String> {
        let translated = translate_sql(sql);

        let body = serde_json::json!({"sql": translated});

        let resp = self
            .http
            .post(&self.url)
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

        let table = flare_resp
            .result_table
            .ok_or_else(|| "FlareDB response missing result_table".to_string())?;

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

        Ok(lines.join("\n"))
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
