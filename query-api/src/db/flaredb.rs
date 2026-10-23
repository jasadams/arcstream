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

const PROFILE_ARRAY_COLUMNS: [&str; 2] = ["top_pages", "top_features"];

fn decode_profile_value(
    name: &str,
    value: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    if !PROFILE_ARRAY_COLUMNS.contains(&name) {
        return Ok(value.clone());
    }
    match value {
        serde_json::Value::Null => Ok(value.clone()),
        serde_json::Value::Array(values) if values.iter().all(serde_json::Value::is_string) => {
            Ok(value.clone())
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
    fn native_profile_arrays_preserve_escapes_empty_and_null() {
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
            rows: vec![vec![values.clone(), json!([]), json!("[ordinary page]")]],
        };
        let decoded: serde_json::Value =
            serde_json::from_str(&table_to_jsonl(&table).unwrap()).unwrap();
        assert_eq!(
            decoded,
            json!({"top_pages": values, "top_features": [], "last_page": "[ordinary page]"})
        );
        assert_eq!(
            decode_profile_value("top_pages", &json!(null)).unwrap(),
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
            json!("[]"),
            json!("[\"valid JSON text\"]"),
            json!("null"),
            json!("not JSON"),
            json!("{}"),
            json!("[1]"),
            json!("[null]"),
            json!(false),
            json!([1]),
            json!([null]),
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
    use crate::schema::{live_profile::LiveProfile, user_profile::UserProfileRow};
    use serde_json::json;

    #[test]
    fn actual_native_http_profile_response_preserves_all_22_literal_fields() {
        let response: FlareDBResponse =
            serde_json::from_str(include_str!("fixtures/flaredb-profile-native-http.json"))
                .unwrap();
        let expected = json!([
            {"tenant_id":"tenant-a","canonical_id":"cf3ebbc9-d5ce-40cc-888c-262e0d437eea",
             "first_seen":"2020-02-01 10:00:02.000","last_seen":"2020-02-01 10:00:07.000",
             "total_events":5,"page_views":1,"clicks":1,"logins":1,"feature_uses":2,
             "user_id":"logged-user","last_page":"/login","last_country":"AU",
             "last_device":"tablet","last_browser":"Firefox","events_1d":5,"events_7d":5,
             "events_30d":5,"events_90d":5,"top_pages":["/home","/login"],
             "top_features":["search","export"],"avg_session_duration_sec":5.0,"action":"update"},
            {"tenant_id":"tenant-b","canonical_id":"f575e62e-2658-4ffc-9c0b-0ccc30237a53",
             "first_seen":"2020-02-01 10:30:13.000","last_seen":"2020-02-01 10:30:13.000",
             "total_events":1,"page_views":1,"clicks":0,"logins":0,"feature_uses":0,
             "user_id":"","last_page":"/other","last_country":"","last_device":"phone",
             "last_browser":"","events_1d":1,"events_7d":1,"events_30d":1,"events_90d":1,
             "top_pages":["/other"],"top_features":[],"avg_session_duration_sec":null,"action":"create"}
        ]);
        let jsonl = table_to_jsonl(&response.result_table).unwrap();
        let rows: Vec<serde_json::Value> = jsonl
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(json!(rows), expected);
        assert_eq!(response.result_table.data_schema.column_names.len(), 22);
        let users = crate::db::pinot::parse_jsonl::<UserProfileRow>(&jsonl);
        assert_eq!(users.len(), 2);
        for (index, row) in response.result_table.rows.iter().enumerate() {
            // The same row decoder serves get_live_profile; query and query_with_stats use JSONL.
            let decoded = decode_result_row(&response.result_table, row).unwrap();
            let profile: LiveProfile =
                serde_json::from_value(serde_json::Value::Object(decoded)).unwrap();
            assert_eq!(profile.first_seen, users[index].first_seen);
            assert_eq!(
                profile.top_pages,
                if index == 0 {
                    vec!["/home", "/login"]
                } else {
                    vec!["/other"]
                }
            );
            assert_eq!(
                profile.avg_session_duration_sec,
                if index == 0 { Some(5.0) } else { None }
            );
        }
    }

    #[test]
    fn historical_string_encoded_array_http_response_is_rejected_unchanged() {
        // Preserve the original scalar-engine receipt as negative evidence, not a native pass.
        let response: FlareDBResponse =
            serde_json::from_str(include_str!("fixtures/flaredb-profile-scalar-http.json"))
                .unwrap();
        assert!(table_to_jsonl(&response.result_table)
            .unwrap_err()
            .contains("top_pages"));
        for row in &response.result_table.rows {
            assert!(decode_result_row(&response.result_table, row).is_err());
        }
    }
}
