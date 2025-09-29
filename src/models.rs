use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Deserialize, Serialize)]
pub struct CaptureRequest {
    #[serde(default)]
    pub api_key: Option<String>,
    pub event: String,
    pub distinct_id: String,
    #[serde(default)]
    pub properties: Option<Value>,
    #[serde(default)]
    pub timestamp: Option<DateTime<Utc>>,
    #[serde(default)]
    pub context: Option<Value>,
    #[serde(default)]
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct IdentifyRequest {
    #[serde(default)]
    pub api_key: Option<String>,
    pub distinct_id: String,
    #[serde(default)]
    pub properties: Option<Value>,
    #[serde(default)]
    pub timestamp: Option<DateTime<Utc>>,
    #[serde(default)]
    pub context: Option<Value>,
    #[serde(default)]
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Serialize)]
pub struct PostHogResponse {
    pub status: u8,
}

impl PostHogResponse {
    pub fn success() -> Self {
        Self { status: 1 }
    }
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub status: u8,
    pub error: String,
}

pub fn hash_map_is_empty(map: &HashMap<String, Value>) -> bool {
    map.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn capture_request_to_pipeline_event() {
        use crate::pipeline::PipelineEvent;

        let mut extra = HashMap::new();
        extra.insert("library".to_string(), Value::String("axum".to_string()));

        let payload = CaptureRequest {
            api_key: Some("phc_test".to_string()),
            event: "test-event".to_string(),
            distinct_id: "abc123".to_string(),
            properties: Some(json!({ "path": "/" })),
            timestamp: Some(Utc.timestamp_millis_opt(1_696_000_000_000).unwrap()),
            context: Some(json!({ "ip": "127.0.0.1" })),
            extra: extra.clone(),
        };

        let event = PipelineEvent::from_capture(payload);

        assert_eq!(
            event,
            PipelineEvent {
                source: "posthog",
                event_type: "test-event".to_string(),
                distinct_id: "abc123".to_string(),
                timestamp: Some(Utc.timestamp_millis_opt(1_696_000_000_000).unwrap()),
                properties: Some(json!({ "path": "/" })),
                context: Some(json!({ "ip": "127.0.0.1" })),
                person_properties: None,
                api_key: Some("phc_test".to_string()),
                extra,
            }
        );
    }

    #[test]
    fn identify_request_to_pipeline_event() {
        use crate::pipeline::PipelineEvent;

        let mut extra = HashMap::new();
        extra.insert("source".to_string(), Value::String("mobile".to_string()));

        let payload = IdentifyRequest {
            api_key: None,
            distinct_id: "user_1".to_string(),
            properties: Some(json!({ "email": "test@example.com" })),
            timestamp: Some(Utc.timestamp_millis_opt(1_696_000_500_000).unwrap()),
            context: None,
            extra: extra.clone(),
        };

        let event = PipelineEvent::from_identify(payload);

        assert_eq!(
            event,
            PipelineEvent {
                source: "posthog",
                event_type: "$identify".to_string(),
                distinct_id: "user_1".to_string(),
                timestamp: Some(Utc.timestamp_millis_opt(1_696_000_500_000).unwrap()),
                properties: None,
                context: None,
                person_properties: Some(json!({ "email": "test@example.com" })),
                api_key: None,
                extra,
            }
        );
    }
}
