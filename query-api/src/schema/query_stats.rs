use std::sync::Mutex;

use crate::db::pinot::QueryResult;

#[derive(serde::Serialize)]
pub struct QueryStatEntry {
    pub sql: String,
    pub backend: String,
    pub elapsed_ms: Option<u64>,
    pub path: Option<String>,
    pub segments_total: Option<usize>,
    pub segments_indexed: Option<usize>,
    pub rows_scanned: Option<usize>,
}

// std::sync::Mutex is intentional: the critical section is a single
// Vec::push / mem::take with no .await, so the guard is never held
// across an await point. tokio::sync::Mutex would add unnecessary
// async overhead here.
pub struct QueryStatsCollector {
    entries: Mutex<Vec<QueryStatEntry>>,
}

impl Default for QueryStatsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryStatsCollector {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
        }
    }

    pub fn push(&self, result: &QueryResult) {
        let entry = match &result.stats {
            Some(stats) => QueryStatEntry {
                sql: result.sql.clone(),
                backend: stats.backend.clone(),
                elapsed_ms: stats.elapsed_ms,
                path: stats.path.clone(),
                segments_total: stats.segments_total,
                segments_indexed: stats.segments_indexed,
                rows_scanned: stats.rows_scanned,
            },
            None => QueryStatEntry {
                sql: result.sql.clone(),
                backend: result.backend.clone(),
                elapsed_ms: None,
                path: None,
                segments_total: None,
                segments_indexed: None,
                rows_scanned: None,
            },
        };

        self.entries.lock().unwrap().push(entry);
    }

    pub fn take(&self) -> Vec<QueryStatEntry> {
        std::mem::take(&mut *self.entries.lock().unwrap())
    }
}
