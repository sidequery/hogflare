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
    #[serde(rename = "$anon_distinct_id")]
    #[serde(default)]
    pub anon_distinct_id: Option<String>,
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

#[derive(Debug, Deserialize, Serialize)]
pub struct BatchRequest {
    #[serde(default)]
    pub api_key: Option<String>,
    pub batch: Vec<Value>,
    #[serde(default)]
    pub sent_at: Option<DateTime<Utc>>,
    #[serde(default)]
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AliasRequest {
    #[serde(default)]
    pub api_key: Option<String>,
    pub distinct_id: String,
    pub alias: String,
    #[serde(default)]
    pub timestamp: Option<DateTime<Utc>>,
    #[serde(default)]
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct EngageRequest {
    #[serde(default)]
    pub api_key: Option<String>,
    pub distinct_id: String,
    #[serde(rename = "$set")]
    #[serde(default)]
    pub set: Option<Value>,
    #[serde(rename = "$set_once")]
    #[serde(default)]
    pub set_once: Option<Value>,
    #[serde(rename = "$unset")]
    #[serde(default)]
    pub unset: Option<Value>,
    #[serde(rename = "$group_set")]
    #[serde(default)]
    pub group_set: Option<Value>,
    #[serde(default)]
    pub timestamp: Option<DateTime<Utc>>,
    #[serde(default)]
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GroupIdentifyRequest {
    #[serde(default)]
    pub api_key: Option<String>,
    pub group_type: String,
    pub group_key: String,
    #[serde(default)]
    pub properties: Option<Value>,
    #[serde(default)]
    pub timestamp: Option<DateTime<Utc>>,
    #[serde(default)]
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Serialize)]
pub struct DecideResponse {
    pub status: u16,
    #[serde(rename = "featureFlags")]
    pub feature_flags: HashMap<String, Value>,
    #[serde(rename = "featureFlagPayloads")]
    pub feature_flag_payloads: HashMap<String, Value>,
    pub config: DecideConfig,
    #[serde(rename = "errorsWhileComputingFlags")]
    pub errors_while_computing_flags: Vec<Value>,
    #[serde(rename = "sessionRecording")]
    pub session_recording: DecideSessionRecording,
    #[serde(rename = "supportedCompression")]
    pub supported_compression: Vec<String>,
}

impl Default for DecideResponse {
    fn default() -> Self {
        Self {
            status: 200,
            feature_flags: HashMap::new(),
            feature_flag_payloads: HashMap::new(),
            config: DecideConfig::default(),
            errors_while_computing_flags: Vec::new(),
            session_recording: DecideSessionRecording::default(),
            supported_compression: vec!["gzip".to_string(), "gzip-js".to_string()],
        }
    }
}

#[derive(Debug, Serialize, Default)]
pub struct DecideConfig {
    #[serde(rename = "enableCollectEverything")]
    pub enable_collect_everything: bool,
    #[serde(rename = "autocapture")]
    pub autocapture: bool,
    #[serde(rename = "apiToken")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_token: Option<String>,
}

#[derive(Debug, Serialize, Default)]
pub struct DecideSessionRecording {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    #[serde(rename = "consoleLogRecordingEnabled")]
    pub console_log_recording_enabled: bool,
    pub proxy: bool,
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

        assert_eq!(event.source, "posthog");
        assert_eq!(event.event, "test-event");
        assert_eq!(event.distinct_id, "abc123");
        assert_eq!(
            event.timestamp,
            Some(Utc.timestamp_millis_opt(1_696_000_000_000).unwrap())
        );
        assert_eq!(event.properties, Some(json!({ "path": "/" })));
        assert_eq!(event.context, Some(json!({ "ip": "127.0.0.1" })));
        assert_eq!(event.person_properties, None);
        assert_eq!(event.api_key, Some("phc_test".to_string()));
        assert_eq!(event.extra, extra);
        assert!(!event.uuid.is_empty());
    }

    #[test]
    fn identify_request_to_pipeline_event() {
        use crate::pipeline::PipelineEvent;

        let mut extra = HashMap::new();
        extra.insert("source".to_string(), Value::String("mobile".to_string()));

        let payload = IdentifyRequest {
            api_key: None,
            distinct_id: "user_1".to_string(),
            anon_distinct_id: None,
            properties: Some(json!({ "email": "test@example.com" })),
            timestamp: Some(Utc.timestamp_millis_opt(1_696_000_500_000).unwrap()),
            context: None,
            extra: extra.clone(),
        };

        let event = PipelineEvent::from_identify(payload);

        assert_eq!(event.source, "posthog");
        assert_eq!(event.event, "$identify");
        assert_eq!(event.distinct_id, "user_1");
        assert_eq!(
            event.timestamp,
            Some(Utc.timestamp_millis_opt(1_696_000_500_000).unwrap())
        );
        assert_eq!(event.properties, None);
        assert_eq!(event.context, None);
        assert_eq!(
            event.person_properties,
            Some(json!({ "email": "test@example.com" }))
        );
        assert_eq!(event.api_key, None);
        assert_eq!(event.extra, extra);
        assert!(!event.uuid.is_empty());
    }

    #[test]
    fn alias_request_serializes() {
        let mut extra = HashMap::new();
        extra.insert("source".to_string(), Value::String("mobile".to_string()));
        let payload = AliasRequest {
            api_key: Some("phc_alias".to_string()),
            distinct_id: "primary".to_string(),
            alias: "secondary".to_string(),
            timestamp: Some(Utc.timestamp_millis_opt(1_696_001_000_000).unwrap()),
            extra: extra.clone(),
        };

        let value = serde_json::to_value(&payload).unwrap();
        assert_eq!(value["distinct_id"], "primary");
        assert_eq!(value["alias"], "secondary");
        assert_eq!(value["api_key"], "phc_alias");
        assert_eq!(value["source"], json!("mobile"));
    }
}
