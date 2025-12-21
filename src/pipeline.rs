use std::time::Duration;

use chrono::{DateTime, Utc};
use http::StatusCode;
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;
use tracing::{info, instrument};
use url::Url;

#[cfg(not(target_arch = "wasm32"))]
use reqwest::Client;

#[cfg(target_arch = "wasm32")]
use worker::{
    AbortController, Delay, Fetch, Headers, Method, Request, RequestInit,
    wasm_bindgen::JsValue, wasm_bindgen_futures::spawn_local,
};

use crate::models::{
    hash_map_is_empty, AliasRequest, CaptureRequest, EngageRequest, GroupIdentifyRequest,
    IdentifyRequest,
};

#[derive(Clone)]
pub struct PipelineClient {
    endpoint: Url,
    auth_token: Option<String>,
    timeout: Duration,
    #[cfg(not(target_arch = "wasm32"))]
    client: Client,
}

impl PipelineClient {
    pub fn new(
        endpoint: Url,
        auth_token: Option<String>,
        timeout: Duration,
    ) -> Result<Self, PipelineError> {
        #[cfg(not(target_arch = "wasm32"))]
        let client = Client::builder()
            .timeout(timeout)
            .build()
            .map_err(PipelineError::ClientBuild)?;

        Ok(Self {
            endpoint,
            auth_token,
            timeout,
            #[cfg(not(target_arch = "wasm32"))]
            client,
        })
    }

    #[instrument(skip(self, events), fields(event_count = events.len()))]
    pub async fn send(&self, events: Vec<PipelineEvent>) -> Result<(), PipelineError> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let mut request = self.client.post(self.endpoint.clone()).json(&events);

            if let Some(token) = &self.auth_token {
                request = request.bearer_auth(token);
            }

            let response = request.send().await.map_err(PipelineError::Transport)?;
            let status = StatusCode::from_u16(response.status().as_u16())
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

            if !status.is_success() {
                let body = response.text().await.unwrap_or_default();
                return Err(PipelineError::UnexpectedResponse { status, body });
            }

            let body = response.text().await.unwrap_or_default();
            info!(
                status = %status,
                response = %body,
                "pipeline request successful"
            );

            return Ok(());
        }

        #[cfg(target_arch = "wasm32")]
        {
            let body = serde_json::to_string(&events).map_err(PipelineError::Serialize)?;

            let mut headers = Headers::new();
            headers
                .set("content-type", "application/json")
                .map_err(PipelineError::RequestBuild)?;

            if let Some(token) = &self.auth_token {
                headers
                    .set("authorization", &format!("Bearer {token}"))
                    .map_err(PipelineError::RequestBuild)?;
            }

            let mut init = RequestInit::new();
            init.with_method(Method::Post);
            init.with_headers(headers);
            init.with_body(Some(JsValue::from_str(&body)));

            let request =
                Request::new_with_init(self.endpoint.as_str(), &init)
                    .map_err(PipelineError::RequestBuild)?;

            let controller = AbortController::default();
            let signal = controller.signal();
            let timeout = self.timeout;

            if timeout.as_secs() > 0 {
                spawn_local(async move {
                    Delay::from(timeout).await;
                    controller.abort();
                });
            }

            let mut response = Fetch::Request(request)
                .send_with_signal(&signal)
                .await
                .map_err(PipelineError::Transport)?;

            let status = StatusCode::from_u16(response.status_code())
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

            if !status.is_success() {
                let body = response.text().await.unwrap_or_default();
                return Err(PipelineError::UnexpectedResponse { status, body });
            }

            let body = response.text().await.unwrap_or_default();
            info!(
                status = %status,
                response = %body,
                "pipeline request successful"
            );

            Ok(())
        }
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

    pub fn from_alias(payload: AliasRequest) -> Self {
        let mut extra = payload.extra;
        extra.insert("alias".to_string(), Value::String(payload.alias.clone()));

        Self {
            source: "posthog",
            event_type: "$create_alias".to_string(),
            distinct_id: payload.distinct_id,
            timestamp: payload.timestamp,
            properties: None,
            context: None,
            person_properties: None,
            api_key: payload.api_key,
            extra,
        }
    }

    pub fn from_engage(payload: EngageRequest) -> Self {
        let mut extra = payload.extra;
        if let Some(set) = payload.set {
            extra.insert("$set".to_string(), set);
        }
        if let Some(set_once) = payload.set_once {
            extra.insert("$set_once".to_string(), set_once);
        }
        if let Some(unset) = payload.unset {
            extra.insert("$unset".to_string(), unset);
        }
        if let Some(group_set) = payload.group_set {
            extra.insert("$group_set".to_string(), group_set);
        }

        Self {
            source: "posthog",
            event_type: "$engage".to_string(),
            distinct_id: payload.distinct_id,
            timestamp: payload.timestamp,
            properties: None,
            context: None,
            person_properties: None,
            api_key: payload.api_key,
            extra,
        }
    }

    pub fn from_session_recording(
        distinct_id: String,
        payload: Value,
        api_key: Option<String>,
    ) -> Self {
        Self {
            source: "posthog",
            event_type: "$snapshot".to_string(),
            distinct_id,
            timestamp: None,
            properties: Some(payload),
            context: None,
            person_properties: None,
            api_key,
            extra: std::collections::HashMap::new(),
        }
    }

    pub fn with_sent_at(mut self, sent_at: Option<DateTime<Utc>>) -> Self {
        if let Some(sent_at) = sent_at {
            self.extra
                .entry("$sent_at".to_string())
                .or_insert(Value::String(sent_at.to_rfc3339()));
        }
        self
    }

}

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error("failed to create HTTP client: {0}")]
    #[cfg(not(target_arch = "wasm32"))]
    ClientBuild(#[source] reqwest::Error),
    #[error("failed to deliver events to Cloudflare pipeline: {0}")]
    #[cfg(not(target_arch = "wasm32"))]
    Transport(#[source] reqwest::Error),
    #[error("failed to deliver events to Cloudflare pipeline: {0}")]
    #[cfg(target_arch = "wasm32")]
    Transport(#[source] worker::Error),
    #[error("failed to build pipeline request: {0}")]
    #[cfg(target_arch = "wasm32")]
    RequestBuild(#[source] worker::Error),
    #[error("failed to serialize pipeline payload: {0}")]
    #[cfg(target_arch = "wasm32")]
    Serialize(#[source] serde_json::Error),
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

    #[test]
    fn converts_alias_payload() {
        let mut extra = std::collections::HashMap::new();
        extra.insert("source".to_string(), Value::String("sdk".to_string()));

        let payload = AliasRequest {
            api_key: Some("phc_alias".to_string()),
            distinct_id: "primary".to_string(),
            alias: "secondary".to_string(),
            timestamp: Some(chrono::Utc.timestamp_millis_opt(1_700_000_000_000).unwrap()),
            extra,
        };

        let event = PipelineEvent::from_alias(payload);
        assert_eq!(event.event_type, "$create_alias");
        assert_eq!(event.distinct_id, "primary");
        assert_eq!(
            event.extra.get("alias"),
            Some(&Value::String("secondary".to_string()))
        );
    }

    #[test]
    fn converts_engage_payload() {
        let payload = EngageRequest {
            api_key: Some("phc_people".to_string()),
            distinct_id: "user_123".to_string(),
            set: Some(json!({ "name": "Alex" })),
            set_once: Some(json!({ "created_at": "2024-01-01" })),
            unset: Some(json!(["tmp"])),
            group_set: Some(json!({ "company": "sidequery" })),
            timestamp: None,
            extra: std::collections::HashMap::new(),
        };

        let event = PipelineEvent::from_engage(payload);
        assert_eq!(event.event_type, "$engage");
        assert_eq!(event.extra.get("$set").unwrap(), &json!({ "name": "Alex" }));
    }

    #[test]
    fn converts_session_recording_payload() {
        let payload = json!({
            "data": {
                "chunk_id": 0,
                "chunk": "base64",
                "metadata": {
                    "distinct_id": "recording-user"
                }
            }
        });

        let event = PipelineEvent::from_session_recording(
            "recording-user".to_string(),
            payload.clone(),
            Some("phc_session".to_string()),
        );

        assert_eq!(event.event_type, "$snapshot");
        assert_eq!(event.distinct_id, "recording-user");
        assert_eq!(event.api_key.as_deref(), Some("phc_session"));
        assert_eq!(event.properties, Some(payload));
    }
}
