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

// Historical FlareDB receipts may still contain changed_fields; preserve their typed-array decoding.
const PROFILE_ARRAY_COLUMNS: [&str; 3] = ["top_pages", "top_features", "changed_fields"];

fn decode_profile_value(
    name: &str,
    value: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    if !PROFILE_ARRAY_COLUMNS.contains(&name) {
        return Ok(value.clone());
    }
    let decoded = match value {
        serde_json::Value::String(text) => serde_json::from_str(text)
            .map_err(|_| format!("Invalid JSON array text in FlareDB profile column {name}"))?,
        other => other.clone(),
    };
    match &decoded {
        serde_json::Value::Null => Ok(decoded),
        serde_json::Value::Array(values) if values.iter().all(serde_json::Value::is_string) => {
            Ok(decoded)
        }
        _ => Err(format!(
            "FlareDB profile column {name} requires an array of strings or null"
        )),
    }
}

fn decode_result_row(
    table: &ResultTable,
    row: &[serde_json::Value],
) -> Result<serde_json::Map<String, serde_json::Value>, String> {
    let mut obj = serde_json::Map::new();
    for (i, name) in table.data_schema.column_names.iter().enumerate() {
        let value = row
            .get(i)
            .ok_or_else(|| format!("Missing FlareDB result column {name}"))?;
        obj.insert(name.clone(), decode_profile_value(name, value)?);
    }
    Ok(obj)
}

fn table_to_jsonl(table: &ResultTable) -> Result<String, String> {
    table
        .rows
        .iter()
        .map(|row| {
            serde_json::to_string(&decode_result_row(table, row)?)
                .map_err(|error| format!("FlareDB result serialization failed: {error}"))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|lines| lines.join("\n"))
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
        table_to_jsonl(&resp.result_table)
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
            body: table_to_jsonl(&resp.result_table)?,
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
             total_events, \
             events_1d, events_7d, events_30d, events_90d, \
             avg_session_duration_sec, \
             page_views, clicks, logins, feature_uses, \
             last_page, last_country, last_device, last_browser, top_pages, top_features \
             FROM profiles \
             WHERE tenant_id = '{tenant_id}' AND canonical_id = '{canonical_id}' \
             LIMIT 1"
        );

        let resp = self.execute(&sql).await?;

        if resp.result_table.rows.is_empty() {
            return Ok(None);
        }

        let obj = decode_result_row(&resp.result_table, &resp.result_table.rows[0])?;

        serde_json::to_string(&obj)
            .map(Some)
            .map_err(|e| format!("JSON serialization failed: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn profile_json_text_arrays_round_trip_escapes_empty_and_null() {
        let values = json!([
            "quote\"",
            "back\\slash",
            "comma,value",
            "line\nfeed",
            "snowman ☃"
        ]);
        let table = ResultTable {
            data_schema: DataSchema {
                column_names: vec![
                    "top_pages".into(),
                    "top_features".into(),
                    "last_page".into(),
                ],
            },
            rows: vec![vec![
                json!(values.to_string()),
                json!("[]"),
                json!("[ordinary page]"),
            ]],
        };
        let decoded: serde_json::Value =
            serde_json::from_str(&table_to_jsonl(&table).unwrap()).unwrap();
        assert_eq!(
            decoded,
            json!({"top_pages": values, "top_features": [], "last_page": "[ordinary page]"})
        );
        assert_eq!(
            decode_profile_value("top_pages", &json!("null")).unwrap(),
            serde_json::Value::Null
        );
        assert_eq!(
            decode_profile_value("top_features", &json!(["already", "typed"])).unwrap(),
            json!(["already", "typed"])
        );
    }

    #[test]
    fn malformed_profile_arrays_fail_in_every_result_path() {
        for value in [
            json!("not JSON"),
            json!("{}"),
            json!("[1]"),
            json!("[null]"),
            json!(false),
            json!([1]),
        ] {
            for name in PROFILE_ARRAY_COLUMNS {
                let table = ResultTable {
                    data_schema: DataSchema {
                        column_names: vec![name.into()],
                    },
                    rows: vec![vec![value.clone()]],
                };
                assert!(table_to_jsonl(&table).is_err(), "accepted {name}: {value}");
                assert!(decode_result_row(&table, &table.rows[0]).is_err());
            }
        }
    }
}

#[cfg(test)]
mod published_http_tests {
    use super::*;
    use crate::schema::{event::Event, live_profile::LiveProfile, user_profile::UserProfileRow};

    #[test]
    fn published_uncast_profile_response_deserializes_application_models() {
        let response: FlareDBResponse =
            serde_json::from_str(include_str!("fixtures/flaredb-profile-scalar-http.json"))
                .unwrap();
        let jsonl = table_to_jsonl(&response.result_table).unwrap();
        let users = crate::db::pinot::parse_jsonl::<UserProfileRow>(&jsonl);
        assert_eq!(users.len(), 3);
        assert_eq!(users[0].first_seen, "2026-01-01 00:00:01.000");
        assert_eq!(users[0].total_events, 1);
        // Row b has the application's nonnullable array contract; rows a/c additionally test storage null preservation.
        let decoded =
            decode_result_row(&response.result_table, &response.result_table.rows[1]).unwrap();
        let profile: LiveProfile =
            serde_json::from_value(serde_json::Value::Object(decoded.clone())).unwrap();
        assert_eq!(profile.first_seen, users[1].first_seen);
        assert_eq!(profile.top_pages, Vec::<String>::new());
        assert_eq!(profile.top_features, vec!["x\"\\☃"]);
        let event: Event = serde_json::from_value(serde_json::json!({
            "event_id":"fixture-event", "event_type":"page_view", "tenant_id":"fixture-tenant",
            "canonical_id":"fixture-b", "event_time": decoded["first_seen"],
            "anonymous_id":null,"user_id":null,"page_url":null,"device_type":null,"browser":null,"country":null
        })).unwrap();
        assert_eq!(event.event_time, "2026-01-01 00:00:01.000");
    }
}
