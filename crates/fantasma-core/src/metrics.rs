use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum MetricGranularity {
    Hour,
    Day,
    Week,
    Month,
    Year,
}

impl MetricGranularity {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hour => "hour",
            Self::Day => "day",
            Self::Week => "week",
            Self::Month => "month",
            Self::Year => "year",
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EventMetric {
    Count,
}

impl EventMetric {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Count => "count",
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionMetric {
    Count,
    DurationTotal,
    NewInstalls,
    ActiveInstalls,
}

impl SessionMetric {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::DurationTotal => "duration_total",
            Self::NewInstalls => "new_installs",
            Self::ActiveInstalls => "active_installs",
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct MetricsBucketWindow {
    pub granularity: MetricGranularity,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct EventMetricsQuery {
    pub project_id: Uuid,
    pub metric: EventMetric,
    pub event: String,
    pub window: MetricsBucketWindow,
    pub filters: BTreeMap<String, String>,
    pub group_by: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct SessionMetricsQuery {
    pub project_id: Uuid,
    pub metric: SessionMetric,
    pub window: MetricsBucketWindow,
    pub filters: BTreeMap<String, String>,
    pub group_by: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct MetricsPoint {
    pub bucket: String,
    pub value: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct MetricsSeries {
    pub dimensions: BTreeMap<String, Option<String>>,
    pub points: Vec<MetricsPoint>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct MetricsResponse {
    pub metric: String,
    pub granularity: MetricGranularity,
    pub group_by: Vec<String>,
    pub series: Vec<MetricsSeries>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct CurrentMetricResponse {
    pub metric: String,
    pub window_seconds: u64,
    pub as_of: DateTime<Utc>,
    pub value: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_metric_response_serializes_shape() {
        let response = CurrentMetricResponse {
            metric: "live_installs".to_owned(),
            window_seconds: 120,
            as_of: DateTime::parse_from_rfc3339("2026-03-17T12:00:00Z")
                .expect("valid timestamp")
                .with_timezone(&Utc),
            value: 7,
        };

        let json = serde_json::to_value(response).expect("serialize response");

        assert_eq!(
            json,
            serde_json::json!({
                "metric": "live_installs",
                "window_seconds": 120,
                "as_of": "2026-03-17T12:00:00Z",
                "value": 7
            })
        );
    }

    #[test]
    fn metrics_response_serializes_ungrouped_series_shape() {
        let response = MetricsResponse {
            metric: "count".to_owned(),
            granularity: MetricGranularity::Day,
            group_by: Vec::new(),
            series: vec![MetricsSeries {
                dimensions: BTreeMap::new(),
                points: vec![MetricsPoint {
                    bucket: "2026-03-01".to_owned(),
                    value: 10,
                }],
            }],
        };

        let json = serde_json::to_value(response).expect("serialize response");

        assert_eq!(
            json,
            serde_json::json!({
                "metric": "count",
                "granularity": "day",
                "group_by": [],
                "series": [
                    {
                        "dimensions": {},
                        "points": [
                            { "bucket": "2026-03-01", "value": 10 }
                        ]
                    }
                ]
            })
        );
    }

    #[test]
    fn metrics_response_serializes_grouped_hourly_series_shape() {
        let response = MetricsResponse {
            metric: "count".to_owned(),
            granularity: MetricGranularity::Hour,
            group_by: vec!["platform".to_owned()],
            series: vec![MetricsSeries {
                dimensions: BTreeMap::from([("platform".to_owned(), Some("ios".to_owned()))]),
                points: vec![MetricsPoint {
                    bucket: "2026-03-01T10:00:00Z".to_owned(),
                    value: 80,
                }],
            }],
        };

        let json = serde_json::to_value(response).expect("serialize response");

        assert_eq!(
            json,
            serde_json::json!({
                "metric": "count",
                "granularity": "hour",
                "group_by": ["platform"],
                "series": [
                    {
                        "dimensions": {
                            "platform": "ios"
                        },
                        "points": [
                            { "bucket": "2026-03-01T10:00:00Z", "value": 80 }
                        ]
                    }
                ]
            })
        );
    }

    #[test]
    fn metrics_response_serializes_weekly_granularity_shape() {
        let response = MetricsResponse {
            metric: "active_installs".to_owned(),
            granularity: MetricGranularity::Week,
            group_by: Vec::new(),
            series: vec![MetricsSeries {
                dimensions: BTreeMap::new(),
                points: vec![MetricsPoint {
                    bucket: "2026-03-02".to_owned(),
                    value: 14,
                }],
            }],
        };

        let json = serde_json::to_value(response).expect("serialize response");

        assert_eq!(
            json,
            serde_json::json!({
                "metric": "active_installs",
                "granularity": "week",
                "group_by": [],
                "series": [
                    {
                        "dimensions": {},
                        "points": [
                            { "bucket": "2026-03-02", "value": 14 }
                        ]
                    }
                ]
            })
        );
    }
}
