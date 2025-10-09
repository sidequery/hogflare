use std::time::Duration;

use chrono::{DateTime, Utc};
use reqwest::{Client, StatusCode, Url};
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;
use tracing::instrument;

use crate::models::{hash_map_is_empty, CaptureRequest, GroupIdentifyRequest, IdentifyRequest};

#[derive(Clone)]
pub struct PipelineClient {
    endpoint: Url,
    auth_token: Option<String>,
    client: Client,
}

impl PipelineClient {
    pub fn new(
        endpoint: Url,
        auth_token: Option<String>,
        timeout: Duration,
    ) -> Result<Self, PipelineError> {
        let client = Client::builder()
            .timeout(timeout)
            .build()
            .map_err(PipelineError::ClientBuild)?;

        Ok(Self {
            endpoint,
            auth_token,
            client,
        })
    }

    #[instrument(skip(self, events))]
    pub async fn send(&self, events: Vec<PipelineEvent>) -> Result<(), PipelineError> {
        let mut request = self.client.post(self.endpoint.clone()).json(&events);

        if let Some(token) = &self.auth_token {
            request = request.bearer_auth(token);
        }

        let response = request.send().await.map_err(PipelineError::Transport)?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(PipelineError::UnexpectedResponse { status, body });
        }

        Ok(())
    }
}

#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct PipelineEvent {
    pub source: &'static str,
    pub event_type: String,
    pub distinct_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub person_properties: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
    #[serde(skip_serializing_if = "hash_map_is_empty")]
    pub extra: std::collections::HashMap<String, Value>,
}

impl PipelineEvent {
    pub fn from_capture(payload: CaptureRequest) -> Self {
        Self {
            source: "posthog",
            event_type: payload.event,
            distinct_id: payload.distinct_id,
            timestamp: payload.timestamp,
            properties: payload.properties,
            context: payload.context,
            person_properties: None,
            api_key: payload.api_key,
            extra: payload.extra,
        }
    }

    pub fn from_identify(payload: IdentifyRequest) -> Self {
        Self {
            source: "posthog",
            event_type: "$identify".to_string(),
            distinct_id: payload.distinct_id,
            timestamp: payload.timestamp,
            properties: None,
            context: payload.context,
            person_properties: payload.properties,
            api_key: payload.api_key,
            extra: payload.extra,
        }
    }

    pub fn from_group_identify(payload: GroupIdentifyRequest) -> Self {
        let mut extra = payload.extra;
        extra.insert(
            "group_type".to_string(),
            Value::String(payload.group_type.clone()),
        );
        extra.insert(
            "group_key".to_string(),
            Value::String(payload.group_key.clone()),
        );

        Self {
            source: "posthog",
            event_type: "$groupidentify".to_string(),
            distinct_id: payload.group_key,
            timestamp: payload.timestamp,
            properties: payload.properties,
            context: None,
            person_properties: None,
            api_key: payload.api_key,
            extra,
        }
    }
}

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error("failed to create HTTP client: {0}")]
    ClientBuild(#[source] reqwest::Error),
    #[error("failed to deliver events to Cloudflare pipeline: {0}")]
    Transport(#[source] reqwest::Error),
    #[error("pipeline responded with {status}: {body}")]
    UnexpectedResponse { status: StatusCode, body: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;

    #[test]
    fn converts_group_identify_payload() {
        let mut extra = std::collections::HashMap::new();
        extra.insert("source".to_string(), Value::String("sdk".to_string()));

        let payload = GroupIdentifyRequest {
            api_key: Some("phc_group".to_string()),
            group_type: "company".to_string(),
            group_key: "group_1".to_string(),
            properties: Some(json!({ "size": 10 })),
            timestamp: Some(chrono::Utc.timestamp_millis_opt(1_700_000_000_000).unwrap()),
            extra,
        };

        let event = PipelineEvent::from_group_identify(payload);

        assert_eq!(event.event_type, "$groupidentify");
        assert_eq!(event.distinct_id, "group_1");
        assert_eq!(event.api_key.as_deref(), Some("phc_group"));
        assert_eq!(event.properties, Some(json!({ "size": 10 })));
        assert!(event.context.is_none());
        assert_eq!(
            event.extra.get("group_type"),
            Some(&Value::String("company".to_string()))
        );
        assert_eq!(
            event.extra.get("group_key"),
            Some(&Value::String("group_1".to_string()))
        );
    }
}
