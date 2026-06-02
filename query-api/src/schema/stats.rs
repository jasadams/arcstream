use async_graphql::{Context, Enum, Object, SimpleObject};
use chrono::{DurationRound, Utc};
use serde::Deserialize;
use std::sync::Arc;

use crate::db::pinot::{parse_jsonl, PinotQuerier};
use crate::schema::query_stats::QueryStatsCollector;

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
pub enum TimeRange {
    Day,
    Week,
    Month,
    Quarter,
}

impl TimeRange {
    fn cutoff_timestamp(self) -> String {
        let seconds: i64 = match self {
            TimeRange::Day => 86_400,
            TimeRange::Week => 7 * 86_400,
            TimeRange::Month => 30 * 86_400,
            TimeRange::Quarter => 90 * 86_400,
        };
        let raw = Utc::now() - chrono::Duration::seconds(seconds);
        let floored = match self {
            TimeRange::Day => raw
                .duration_trunc(chrono::Duration::hours(1))
                .unwrap_or(raw),
            _ => raw
                .duration_trunc(chrono::Duration::days(1))
                .unwrap_or(raw),
        };
        floored.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
    }

    fn bucket_granularity(self) -> &'static str {
        match self {
            TimeRange::Day => "hour",
            TimeRange::Week => "day",
            TimeRange::Month => "day",
            TimeRange::Quarter => "week",
        }
    }

    fn events_time_column(self) -> &'static str {
        match self {
            TimeRange::Day => "event_hour",
            _ => "event_date",
        }
    }

    fn sessions_time_column(self) -> &'static str {
        match self {
            TimeRange::Day => "session_hour",
            _ => "session_date",
        }
    }

    fn query_limit(self) -> u32 {
        match self {
            TimeRange::Day => 24,
            TimeRange::Week => 7,
            TimeRange::Month => 30,
            TimeRange::Quarter => 90,
        }
    }

    fn profiles_filter(self) -> String {
        format!("last_seen > '{}'", self.cutoff_timestamp())
    }

    fn events_filter(self) -> String {
        let cutoff = self.cutoff_timestamp();
        match self {
            TimeRange::Day => {
                let hour = &cutoff[..13];
                format!("event_hour >= '{hour}'")
            }
            _ => {
                let date = &cutoff[..10];
                format!("event_date >= '{date}'")
            }
        }
    }

    fn sessions_filter(self) -> String {
        let cutoff = self.cutoff_timestamp();
        match self {
            TimeRange::Day => {
                let hour = &cutoff[..13];
                format!("session_hour >= '{hour}'")
            }
            _ => {
                let date = &cutoff[..10];
                format!("session_date >= '{date}'")
            }
        }
    }
}

#[derive(SimpleObject, Clone)]
pub struct TimeSeriesPoint {
    pub bucket: String,
    pub value: f64,
}

#[derive(SimpleObject, Clone)]
pub struct GroupedTimeSeriesPoint {
    pub bucket: String,
    pub group: String,
    pub value: f64,
}

#[derive(SimpleObject, Deserialize)]
pub struct BreakdownRow {
    pub label: String,
    pub value: f64,
}

#[derive(SimpleObject)]
pub struct DashboardStats {
    pub total_users: u64,
    pub total_events: u64,
    pub active_sessions: u64,
}

#[derive(SimpleObject)]
pub struct AnalyticsSummary {
    pub users: u64,
    pub sessions: u64,
    pub events: u64,
    pub avg_duration_sec: f64,
    pub events_per_session: f64,
}

#[derive(Default)]
pub struct StatsQuery;

#[Object]
impl StatsQuery {
    async fn dashboard_stats(&self, ctx: &Context<'_>) -> async_graphql::Result<DashboardStats> {
        let pinot = ctx.data::<Arc<dyn PinotQuerier>>()?;

        let users_sql = "SELECT COUNT(*) AS total_users FROM profiles";
        let events_sql = "SELECT COUNT(*) AS total_events FROM events";
        let thirty_min_ago = (Utc::now() - chrono::Duration::seconds(1800))
            .format("%Y-%m-%d %H:%M:%S%.3f")
            .to_string();
        let sessions_sql = format!(
            "SELECT DISTINCTCOUNTHLL(session_id) AS active_sessions FROM events WHERE event_time > '{thirty_min_ago}'"
        );

        let (users_result, events_result, sessions_result) = tokio::try_join!(
            pinot.query_with_stats(users_sql),
            pinot.query_with_stats(events_sql),
            pinot.query_with_stats(&sessions_sql),
        )
        .map_err(async_graphql::Error::new)?;

        if let Ok(collector) = ctx.data::<Arc<QueryStatsCollector>>() {
            collector.push(&users_result);
            collector.push(&events_result);
            collector.push(&sessions_result);
        }

        #[derive(Deserialize)]
        struct UsersRow {
            total_users: u64,
        }
        #[derive(Deserialize)]
        struct EventsRow {
            total_events: u64,
        }
        #[derive(Deserialize)]
        struct SessionsRow {
            active_sessions: u64,
        }

        let users: Vec<UsersRow> = parse_jsonl(&users_result.body);
        let events: Vec<EventsRow> = parse_jsonl(&events_result.body);
        let sessions: Vec<SessionsRow> = parse_jsonl(&sessions_result.body);

        Ok(DashboardStats {
            total_users: users.first().map(|r| r.total_users).unwrap_or(0),
            total_events: events.first().map(|r| r.total_events).unwrap_or(0),
            active_sessions: sessions.first().map(|r| r.active_sessions).unwrap_or(0),
        })
    }

    async fn analytics_summary(
        &self,
        ctx: &Context<'_>,
        range: TimeRange,
    ) -> async_graphql::Result<AnalyticsSummary> {
        let pinot = ctx.data::<Arc<dyn PinotQuerier>>()?;
        let ef = range.events_filter();
        let sf = range.sessions_filter();

        let pf = range.profiles_filter();
        let users_sql = format!(
            "SELECT COUNT(*) AS users FROM profiles WHERE {pf}"
        );
        let sessions_sql = format!(
            "SELECT COUNT(*) AS sessions FROM sessions WHERE {sf}"
        );
        let events_sql = format!(
            "SELECT COUNT(*) AS events FROM events WHERE {ef}"
        );
        let avg_duration_sql = format!(
            "SELECT AVG(duration_sec) AS avg_duration_sec FROM sessions WHERE {sf} AND duration_sec > 0"
        );

        let (users_result, sessions_result, events_result, avg_duration_result) = tokio::try_join!(
            pinot.query_with_stats(&users_sql),
            pinot.query_with_stats(&sessions_sql),
            pinot.query_with_stats(&events_sql),
            pinot.query_with_stats(&avg_duration_sql),
        )
        .map_err(async_graphql::Error::new)?;

        if let Ok(collector) = ctx.data::<Arc<QueryStatsCollector>>() {
            collector.push(&users_result);
            collector.push(&sessions_result);
            collector.push(&events_result);
            collector.push(&avg_duration_result);
        }

        #[derive(Deserialize)]
        struct UsersRow {
            users: u64,
        }
        #[derive(Deserialize)]
        struct SessionsRow {
            sessions: u64,
        }
        #[derive(Deserialize)]
        struct EventsRow {
            events: u64,
        }
        #[derive(Deserialize)]
        struct AvgDurationRow {
            avg_duration_sec: f64,
        }

        let users: Vec<UsersRow> = parse_jsonl(&users_result.body);
        let sessions: Vec<SessionsRow> = parse_jsonl(&sessions_result.body);
        let events: Vec<EventsRow> = parse_jsonl(&events_result.body);
        let avg_duration: Vec<AvgDurationRow> = parse_jsonl(&avg_duration_result.body);

        let users_val = users.first().map(|r| r.users).unwrap_or(0);
        let sessions_val = sessions.first().map(|r| r.sessions).unwrap_or(0);
        let events_val = events.first().map(|r| r.events).unwrap_or(0);
        let avg_duration_val = avg_duration.first().map(|r| r.avg_duration_sec).unwrap_or(0.0);
        let events_per_session = if sessions_val == 0 {
            0.0
        } else {
            events_val as f64 / sessions_val as f64
        };

        Ok(AnalyticsSummary {
            users: users_val,
            sessions: sessions_val,
            events: events_val,
            avg_duration_sec: avg_duration_val,
            events_per_session,
        })
    }

    async fn events_over_time(
        &self,
        ctx: &Context<'_>,
        range: TimeRange,
    ) -> async_graphql::Result<Vec<GroupedTimeSeriesPoint>> {
        let pinot = ctx.data::<Arc<dyn PinotQuerier>>()?;
        let filter = range.events_filter();
        let granularity = range.bucket_granularity();
        let time_col = range.events_time_column();

        let limit = range.query_limit() * 20;
        let sql = format!(
            "SELECT {time_col} AS time_bucket, \
             event_type, \
             COUNT(*) AS value \
             FROM events \
             WHERE {filter} \
             GROUP BY time_bucket, event_type \
             ORDER BY time_bucket ASC, event_type ASC \
             LIMIT {limit}"
        );

        let result = pinot.query_with_stats(&sql).await.map_err(async_graphql::Error::new)?;
        if let Ok(collector) = ctx.data::<Arc<QueryStatsCollector>>() {
            collector.push(&result);
        }

        #[derive(Deserialize)]
        struct Row {
            time_bucket: String,
            event_type: String,
            value: f64,
        }
        let rows: Vec<Row> = parse_jsonl(&result.body);
        let points: Vec<GroupedTimeSeriesPoint> = rows
            .into_iter()
            .map(|r| GroupedTimeSeriesPoint {
                bucket: format_time_bucket(&r.time_bucket, granularity),
                group: r.event_type,
                value: r.value,
            })
            .collect();

        if matches!(range, TimeRange::Quarter) {
            Ok(rollup_grouped_to_weekly(points))
        } else {
            Ok(points)
        }
    }

    async fn users_over_time(
        &self,
        ctx: &Context<'_>,
        range: TimeRange,
    ) -> async_graphql::Result<Vec<TimeSeriesPoint>> {
        let pinot = ctx.data::<Arc<dyn PinotQuerier>>()?;
        let filter = range.events_filter();
        let granularity = range.bucket_granularity();
        let time_col = range.events_time_column();

        let limit = range.query_limit();
        let sql = format!(
            "SELECT {time_col} AS time_bucket, \
             DISTINCTCOUNTHLL(canonical_id) AS value \
             FROM events \
             WHERE {filter} \
             GROUP BY time_bucket \
             ORDER BY time_bucket ASC \
             LIMIT {limit}"
        );

        let result = pinot.query_with_stats(&sql).await.map_err(async_graphql::Error::new)?;
        if let Ok(collector) = ctx.data::<Arc<QueryStatsCollector>>() {
            collector.push(&result);
        }

        #[derive(Deserialize)]
        struct Row {
            time_bucket: String,
            value: f64,
        }
        let rows: Vec<Row> = parse_jsonl(&result.body);
        let points: Vec<TimeSeriesPoint> = rows
            .into_iter()
            .map(|r| TimeSeriesPoint {
                bucket: format_time_bucket(&r.time_bucket, granularity),
                value: r.value,
            })
            .collect();

        if matches!(range, TimeRange::Quarter) {
            Ok(rollup_to_weekly(points))
        } else {
            Ok(points)
        }
    }

    async fn sessions_over_time(
        &self,
        ctx: &Context<'_>,
        range: TimeRange,
    ) -> async_graphql::Result<Vec<TimeSeriesPoint>> {
        let pinot = ctx.data::<Arc<dyn PinotQuerier>>()?;
        let filter = range.sessions_filter();
        let granularity = range.bucket_granularity();
        let time_col = range.sessions_time_column();

        let limit = range.query_limit();
        let sql = format!(
            "SELECT {time_col} AS time_bucket, \
             COUNT(*) AS value \
             FROM sessions \
             WHERE {filter} \
             GROUP BY time_bucket \
             ORDER BY time_bucket ASC \
             LIMIT {limit}"
        );

        let result = pinot.query_with_stats(&sql).await.map_err(async_graphql::Error::new)?;
        if let Ok(collector) = ctx.data::<Arc<QueryStatsCollector>>() {
            collector.push(&result);
        }

        #[derive(Deserialize)]
        struct Row {
            time_bucket: String,
            value: f64,
        }
        let rows: Vec<Row> = parse_jsonl(&result.body);
        let points: Vec<TimeSeriesPoint> = rows
            .into_iter()
            .map(|r| TimeSeriesPoint {
                bucket: format_time_bucket(&r.time_bucket, granularity),
                value: r.value,
            })
            .collect();

        if matches!(range, TimeRange::Quarter) {
            Ok(rollup_to_weekly(points))
        } else {
            Ok(points)
        }
    }

    async fn avg_session_duration(
        &self,
        ctx: &Context<'_>,
        range: TimeRange,
    ) -> async_graphql::Result<Vec<TimeSeriesPoint>> {
        let pinot = ctx.data::<Arc<dyn PinotQuerier>>()?;
        let filter = range.sessions_filter();
        let granularity = range.bucket_granularity();
        let time_col = range.sessions_time_column();

        let limit = range.query_limit();
        let sql = format!(
            "SELECT {time_col} AS time_bucket, \
             AVG(duration_sec) AS value \
             FROM sessions \
             WHERE {filter} AND duration_sec > 0 \
             GROUP BY time_bucket \
             ORDER BY time_bucket ASC \
             LIMIT {limit}"
        );

        let result = pinot.query_with_stats(&sql).await.map_err(async_graphql::Error::new)?;
        if let Ok(collector) = ctx.data::<Arc<QueryStatsCollector>>() {
            collector.push(&result);
        }

        #[derive(Deserialize)]
        struct Row {
            time_bucket: String,
            value: f64,
        }
        let rows: Vec<Row> = parse_jsonl(&result.body);
        let points: Vec<TimeSeriesPoint> = rows
            .into_iter()
            .map(|r| TimeSeriesPoint {
                bucket: format_time_bucket(&r.time_bucket, granularity),
                value: (r.value * 10.0).round() / 10.0,
            })
            .collect();

        if matches!(range, TimeRange::Quarter) {
            Ok(rollup_avg_to_weekly(points))
        } else {
            Ok(points)
        }
    }

    async fn top_pages(
        &self,
        ctx: &Context<'_>,
        range: TimeRange,
    ) -> async_graphql::Result<Vec<BreakdownRow>> {
        let pinot = ctx.data::<Arc<dyn PinotQuerier>>()?;
        let filter = range.events_filter();

        let sql = format!(
            "SELECT page_url AS label, COUNT(*) AS value \
             FROM events \
             WHERE {filter} AND page_url != '' \
             GROUP BY label \
             ORDER BY value DESC \
             LIMIT 10"
        );

        let result = pinot.query_with_stats(&sql).await.map_err(async_graphql::Error::new)?;
        if let Ok(collector) = ctx.data::<Arc<QueryStatsCollector>>() {
            collector.push(&result);
        }
        Ok(parse_jsonl(&result.body))
    }

    async fn device_breakdown(
        &self,
        ctx: &Context<'_>,
        range: TimeRange,
    ) -> async_graphql::Result<Vec<BreakdownRow>> {
        let pinot = ctx.data::<Arc<dyn PinotQuerier>>()?;
        let filter = range.events_filter();

        let sql = format!(
            "SELECT device_type AS label, COUNT(*) AS value \
             FROM events \
             WHERE {filter} AND device_type != '' \
             GROUP BY label \
             ORDER BY value DESC"
        );

        let result = pinot.query_with_stats(&sql).await.map_err(async_graphql::Error::new)?;
        if let Ok(collector) = ctx.data::<Arc<QueryStatsCollector>>() {
            collector.push(&result);
        }
        Ok(parse_jsonl(&result.body))
    }

    async fn browser_breakdown(
        &self,
        ctx: &Context<'_>,
        range: TimeRange,
    ) -> async_graphql::Result<Vec<BreakdownRow>> {
        let pinot = ctx.data::<Arc<dyn PinotQuerier>>()?;
        let filter = range.events_filter();

        let sql = format!(
            "SELECT browser AS label, COUNT(*) AS value \
             FROM events \
             WHERE {filter} AND browser != '' \
             GROUP BY label \
             ORDER BY value DESC \
             LIMIT 10"
        );

        let result = pinot.query_with_stats(&sql).await.map_err(async_graphql::Error::new)?;
        if let Ok(collector) = ctx.data::<Arc<QueryStatsCollector>>() {
            collector.push(&result);
        }
        Ok(parse_jsonl(&result.body))
    }

    async fn country_breakdown(
        &self,
        ctx: &Context<'_>,
        range: TimeRange,
    ) -> async_graphql::Result<Vec<BreakdownRow>> {
        let pinot = ctx.data::<Arc<dyn PinotQuerier>>()?;
        let filter = range.events_filter();

        let sql = format!(
            "SELECT country AS label, COUNT(*) AS value \
             FROM events \
             WHERE {filter} AND country != '' \
             GROUP BY label \
             ORDER BY value DESC \
             LIMIT 10"
        );

        let result = pinot.query_with_stats(&sql).await.map_err(async_graphql::Error::new)?;
        if let Ok(collector) = ctx.data::<Arc<QueryStatsCollector>>() {
            collector.push(&result);
        }
        Ok(parse_jsonl(&result.body))
    }
}

fn format_time_bucket(bucket: &str, granularity: &str) -> String {
    use chrono::{DateTime, Utc};

    if bucket.len() >= 13 && bucket.bytes().all(|b| b.is_ascii_digit()) {
        if let Ok(millis) = bucket.parse::<i64>() {
            if let Some(dt) = DateTime::<Utc>::from_timestamp_millis(millis) {
                return match granularity {
                    "hour" => dt.format("%H:00").to_string(),
                    _ => dt.format("%b %d").to_string(),
                };
            }
        }
    }

    match granularity {
        "hour" => {
            if bucket.len() >= 13 {
                format!("{}:00", &bucket[11..13])
            } else {
                bucket.to_string()
            }
        }
        _ => {
            use chrono::NaiveDate;
            NaiveDate::parse_from_str(&bucket[..10.min(bucket.len())], "%Y-%m-%d")
                .map(|d| d.format("%b %d").to_string())
                .unwrap_or_else(|_| bucket.to_string())
        }
    }
}

fn rollup_to_weekly(daily_points: Vec<TimeSeriesPoint>) -> Vec<TimeSeriesPoint> {
    use std::collections::BTreeMap;

    let mut weeks: BTreeMap<String, f64> = BTreeMap::new();
    for p in daily_points {
        let week_start = parse_bucket_to_week_start(&p.bucket);
        *weeks.entry(week_start).or_default() += p.value;
    }
    weeks
        .into_iter()
        .map(|(bucket, value)| TimeSeriesPoint { bucket, value })
        .collect()
}

fn rollup_grouped_to_weekly(
    daily_points: Vec<GroupedTimeSeriesPoint>,
) -> Vec<GroupedTimeSeriesPoint> {
    use std::collections::BTreeMap;

    let mut weeks: BTreeMap<(String, String), f64> = BTreeMap::new();
    for p in daily_points {
        let week_start = parse_bucket_to_week_start(&p.bucket);
        *weeks.entry((week_start, p.group)).or_default() += p.value;
    }
    weeks
        .into_iter()
        .map(|((bucket, group), value)| GroupedTimeSeriesPoint {
            bucket,
            group,
            value,
        })
        .collect()
}

fn rollup_avg_to_weekly(daily_points: Vec<TimeSeriesPoint>) -> Vec<TimeSeriesPoint> {
    use std::collections::BTreeMap;

    let mut weeks: BTreeMap<String, (f64, usize)> = BTreeMap::new();
    for p in daily_points {
        let week_start = parse_bucket_to_week_start(&p.bucket);
        let entry = weeks.entry(week_start).or_default();
        entry.0 += p.value;
        entry.1 += 1;
    }
    weeks
        .into_iter()
        .map(|(bucket, (sum, count))| TimeSeriesPoint {
            bucket,
            value: ((sum / count as f64) * 10.0).round() / 10.0,
        })
        .collect()
}

fn parse_bucket_to_week_start(bucket: &str) -> String {
    use chrono::{Datelike, NaiveDate};

    NaiveDate::parse_from_str(bucket, "%b %d")
        .or_else(|_| NaiveDate::parse_from_str(&format!("{} 2026", bucket), "%b %d %Y"))
        .map(|d| {
            let iso = d.iso_week();
            NaiveDate::from_isoywd_opt(iso.year(), iso.week(), chrono::Weekday::Mon)
                .unwrap_or(d)
                .format("%b %d")
                .to_string()
        })
        .unwrap_or_else(|_| bucket.to_string())
}
