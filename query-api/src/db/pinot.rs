use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct QueryStats {
    pub elapsed_ms: Option<u64>,
    pub path: Option<String>,
    pub segments_total: Option<usize>,
    pub segments_indexed: Option<usize>,
    pub rows_scanned: Option<usize>,
    pub backend: String,
}

pub struct QueryResult {
    pub body: String,
    pub stats: Option<QueryStats>,
    pub sql: String,
    pub backend: String,
}

#[async_trait]
pub trait PinotQuerier: Send + Sync {
    async fn query(&self, sql: &str) -> Result<String, String>;

    async fn query_with_stats(&self, sql: &str) -> Result<QueryResult, String> {
        let body = self.query(sql).await?;
        Ok(QueryResult {
            body,
            stats: None,
            sql: sql.to_owned(),
            backend: "pinot".to_owned(),
        })
    }
}

pub struct PinotClient {
    pub broker_url: String,
    pub http: Client,
}

#[derive(Deserialize)]
struct PinotResponse {
    #[serde(default)]
    exceptions: Vec<PinotException>,
    #[serde(rename = "resultTable")]
    result_table: Option<ResultTable>,
}

#[derive(Deserialize)]
struct PinotException {
    message: String,
}

#[derive(Deserialize)]
struct ResultTable {
    #[serde(rename = "dataSchema")]
    data_schema: DataSchema,
    rows: Vec<Vec<serde_json::Value>>,
}

#[derive(Deserialize)]
struct DataSchema {
    #[serde(rename = "columnNames")]
    column_names: Vec<String>,
}

#[async_trait]
impl PinotQuerier for PinotClient {
    async fn query(&self, sql: &str) -> Result<String, String> {
        let body = serde_json::json!({"sql": sql});

        let resp = self
            .http
            .post(&self.broker_url)
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Pinot request failed: {e}"))?;

        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            eprintln!("Pinot error: {text}");
            return Err(format!("Pinot query failed: {text}"));
        }

        let pinot_resp: PinotResponse = resp
            .json()
            .await
            .map_err(|e| format!("Failed to parse Pinot response: {e}"))?;

        if let Some(exc) = pinot_resp.exceptions.first() {
            return Err(format!("Pinot query error: {}", exc.message));
        }

        let table = pinot_resp
            .result_table
            .ok_or_else(|| "Pinot response missing resultTable".to_string())?;

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

pub fn parse_jsonl<T: serde::de::DeserializeOwned>(body: &str) -> Vec<T> {
    body.lines()
        .filter(|l| !l.is_empty())
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect()
}

pub fn sanitize_input(input: &str) -> Result<String, String> {
    if input
        .chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_' || c == '.')
    {
        Ok(input.to_string())
    } else {
        Err(format!("Invalid input: {input}"))
    }
}

pub fn sanitize_timestamp(input: &str) -> Result<String, String> {
    let bytes = input.as_bytes();
    if bytes.len() < 10 || bytes.len() > 30 {
        return Err(format!("Invalid timestamp length: {input}"));
    }
    if bytes.iter().all(|&b| {
        b.is_ascii_digit()
            || b == b'-'
            || b == b':'
            || b == b'T'
            || b == b' '
            || b == b'.'
            || b == b'Z'
    }) {
        Ok(input.to_string())
    } else {
        Err(format!("Invalid timestamp: {input}"))
    }
}
