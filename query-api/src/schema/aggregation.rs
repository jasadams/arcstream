use async_graphql::{Enum, SimpleObject};
use serde::Deserialize;

#[derive(SimpleObject, Deserialize)]
pub struct EventTypeSummary {
    pub event_type: String,
    pub total_events: u64,
    pub unique_users: u64,
}

#[derive(SimpleObject, Deserialize)]
pub struct GroupCount {
    pub key: String,
    pub count: u64,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
pub enum Dimension {
    Country,
    DeviceType,
    Browser,
    Os,
    EventType,
    PageUrl,
}

impl Dimension {
    pub fn to_column(self) -> &'static str {
        match self {
            Dimension::Country => "country",
            Dimension::DeviceType => "device_type",
            Dimension::Browser => "browser",
            Dimension::Os => "os",
            Dimension::EventType => "event_type",
            Dimension::PageUrl => "page_url",
        }
    }
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
pub enum Metric {
    Count,
    UniqueUsers,
    UniqueSessions,
}

impl Metric {
    pub fn to_sql(self) -> &'static str {
        match self {
            Metric::Count => "COUNT(*)",
            Metric::UniqueUsers => "DISTINCTCOUNTHLL(user_id)",
            Metric::UniqueSessions => "DISTINCTCOUNTHLL(session_id)",
        }
    }
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
pub enum UserSort {
    FirstSeen,
    LastSeen,
    TotalEvents,
    TotalSessions,
}

impl UserSort {
    pub fn to_column(self) -> &'static str {
        match self {
            UserSort::FirstSeen => "first_seen",
            UserSort::LastSeen => "last_seen",
            UserSort::TotalEvents => "total_events",
            UserSort::TotalSessions => "total_sessions",
        }
    }
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
pub enum SortOrder {
    Asc,
    Desc,
}

impl SortOrder {
    pub fn to_sql(self) -> &'static str {
        match self {
            SortOrder::Asc => "ASC",
            SortOrder::Desc => "DESC",
        }
    }
}
