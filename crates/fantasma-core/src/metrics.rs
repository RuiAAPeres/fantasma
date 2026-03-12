use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SessionMetricQuery {
    pub project_id: Uuid,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DailyMetricQuery {
    pub project_id: Uuid,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct EventMetricsDateWindow {
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct EventMetricsQuery {
    pub project_id: Uuid,
    pub event: String,
    pub window: EventMetricsDateWindow,
    pub filters: BTreeMap<String, String>,
    pub group_by: Vec<String>,
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

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct EventMetricsAggregateRow {
    pub dimensions: BTreeMap<String, Option<String>>,
    pub value: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct EventMetricsAggregateResponse {
    pub metric: String,
    pub group_by: Vec<String>,
    pub rows: Vec<EventMetricsAggregateRow>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct EventMetricsPoint {
    pub date: NaiveDate,
    pub value: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct EventMetricsDailySeries {
    pub dimensions: BTreeMap<String, Option<String>>,
    pub points: Vec<EventMetricsPoint>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct EventMetricsDailyResponse {
    pub metric: String,
    pub group_by: Vec<String>,
    pub series: Vec<EventMetricsDailySeries>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_metrics_aggregate_response_serializes_with_rows() {
        let response = EventMetricsAggregateResponse {
            metric: "event_count".to_owned(),
            group_by: vec!["provider".to_owned()],
            rows: vec![EventMetricsAggregateRow {
                dimensions: BTreeMap::from([("provider".to_owned(), Some("strava".to_owned()))]),
                value: 7,
            }],
        };

        let json = serde_json::to_value(response).expect("serialize response");

        assert_eq!(
            json,
            serde_json::json!({
                "metric": "event_count",
                "group_by": ["provider"],
                "rows": [
                    {
                        "dimensions": {
                            "provider": "strava"
                        },
                        "value": 7
                    }
                ]
            })
        );
    }

    #[test]
    fn event_metrics_daily_response_serializes_with_series() {
        let response = EventMetricsDailyResponse {
            metric: "event_count_daily".to_owned(),
            group_by: vec!["provider".to_owned()],
            series: vec![EventMetricsDailySeries {
                dimensions: BTreeMap::from([("provider".to_owned(), Some("strava".to_owned()))]),
                points: vec![EventMetricsPoint {
                    date: serde_json::from_value(serde_json::json!("2026-03-01")).expect("date"),
                    value: 10,
                }],
            }],
        };

        let json = serde_json::to_value(response).expect("serialize response");

        assert_eq!(
            json,
            serde_json::json!({
                "metric": "event_count_daily",
                "group_by": ["provider"],
                "series": [
                    {
                        "dimensions": {
                            "provider": "strava"
                        },
                        "points": [
                            { "date": "2026-03-01", "value": 10 }
                        ]
                    }
                ]
            })
        );
    }
}
