use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::events::Platform;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MetricQuery {
    pub start: NaiveDate,
    pub end: NaiveDate,
    pub platform: Option<Platform>,
    pub app_version: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EventCountQuery {
    pub project_id: Uuid,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct MetricSeriesPoint {
    pub date: NaiveDate,
    pub value: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct MetricResponse {
    pub metric: String,
    pub points: Vec<MetricSeriesPoint>,
}
