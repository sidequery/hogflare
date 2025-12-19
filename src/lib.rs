pub mod config;
pub mod extractors;
pub mod models;
pub mod pipeline;

use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tracing::Level;
use config::{Config, ConfigError};
use extractors::{
    header_api_key, header_sent_at, verify_signature, PostHogBatchPayload, PostHogPayload,
};
use models::{
    AliasRequest, BatchRequest, CaptureRequest, DecideResponse, EngageRequest, ErrorResponse,
    GroupIdentifyRequest, IdentifyRequest, PostHogResponse,
};
use pipeline::{PipelineClient, PipelineError, PipelineEvent};
use serde::Deserialize;
use serde_json::{json, Value};
use thiserror::Error;
use tokio::net::TcpListener;
use tracing::{error, info, warn};

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) pipeline: Arc<PipelineClient>,
    pub(crate) decide_api_token: Option<String>,
    pub(crate) session_recording_endpoint: Option<String>,
    pub(crate) signing_secret: Option<String>,
}

#[derive(Debug, Error)]
enum AppError {
    #[error(transparent)]
    Pipeline(#[from] PipelineError),
    #[error("invalid payload: {0}")]
    InvalidPayload(String),
    #[error("unauthorized: {0}")]
    Unauthorized(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let (status, message) = match &self {
            AppError::Pipeline(err) => {
                error!(error = %err, "request failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error".to_string(),
                )
            }
            AppError::InvalidPayload(err) => {
                warn!(error = %err, "invalid request payload");
                (StatusCode::BAD_REQUEST, err.clone())
            }
            AppError::Unauthorized(err) => {
                warn!(error = %err, "unauthorized request");
                (StatusCode::UNAUTHORIZED, err.clone())
            }
        };

        let body = Json(ErrorResponse {
            status: 0,
            error: message,
        });

        (status, body).into_response()
    }
}

impl From<extractors::PayloadExtractorError> for AppError {
    fn from(error: extractors::PayloadExtractorError) -> Self {
        match error {
            extractors::PayloadExtractorError::MissingSignature
            | extractors::PayloadExtractorError::InvalidSignature => {
                AppError::Unauthorized(error.to_string())
            }
            _ => AppError::InvalidPayload(error.to_string()),
        }
    }
}

pub async fn run() -> Result<(), RunError> {
    // Load .env.local first, then .env as fallback
    dotenvy::from_filename(".env.local")
        .or_else(|_| dotenvy::dotenv())
        .ok();
    init_tracing();

    let config = Config::from_env()?;
    run_with_config(config).await
}

pub async fn run_with_config(config: Config) -> Result<(), RunError> {
    let pipeline = PipelineClient::new(
        config.pipeline_endpoint.clone(),
        config.pipeline_auth_token.clone(),
        config.pipeline_timeout,
    )?;

    info!(
        endpoint = %config.pipeline_endpoint,
        auth_configured = config.pipeline_auth_token.is_some(),
        timeout_secs = config.pipeline_timeout.as_secs(),
        "pipeline client configured"
    );

    let listener = TcpListener::bind(config.address).await?;
    info!(address = %config.address, "listening for requests");

    serve_with_options(
        listener,
        Arc::new(pipeline),
        config.posthog_project_api_key.clone(),
        config.session_recording_endpoint.clone(),
        config.posthog_signing_secret.clone(),
    )
    .await
}

pub fn build_router(pipeline: Arc<PipelineClient>) -> Router {
    build_router_with_options(pipeline, None, None, None)
}

pub fn build_router_with_options(
    pipeline: Arc<PipelineClient>,
    decide_api_token: Option<String>,
    session_recording_endpoint: Option<String>,
    signing_secret: Option<String>,
) -> Router {
    router(build_state(
        pipeline,
        decide_api_token,
        session_recording_endpoint,
        signing_secret,
    ))
}

pub async fn serve(listener: TcpListener, pipeline: Arc<PipelineClient>) -> Result<(), RunError> {
    serve_with_state(listener, build_state(pipeline, None, None, None)).await
}

pub async fn serve_with_options(
    listener: TcpListener,
    pipeline: Arc<PipelineClient>,
    decide_api_token: Option<String>,
    session_recording_endpoint: Option<String>,
    signing_secret: Option<String>,
) -> Result<(), RunError> {
    let state = build_state(
        pipeline,
        decide_api_token,
        session_recording_endpoint,
        signing_secret,
    );
    serve_with_state(listener, state).await
}

fn router(state: AppState) -> Router {
    Router::new()
        .route("/capture", post(capture))
        .route("/e", post(browser_capture))
        .route("/e/", post(browser_capture))
        .route("/identify", post(identify))
        .route("/batch", post(batch))
        .route("/groups", post(groups))
        .route("/alias", post(alias))
        .route("/engage", post(engage))
        .route("/decide", post(decide))
        .route("/flags", post(decide))
        .route("/flags/", post(decide))
        .route("/s", post(session_recording))
        .route("/s/", post(session_recording))
        .route("/healthz", get(health))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
                .on_response(DefaultOnResponse::new().level(Level::INFO)),
        )
        .with_state(state)
}

fn build_state(
    pipeline: Arc<PipelineClient>,
    decide_api_token: Option<String>,
    session_recording_endpoint: Option<String>,
    signing_secret: Option<String>,
) -> AppState {
    AppState {
        pipeline,
        decide_api_token,
        session_recording_endpoint,
        signing_secret,
    }
}

async fn serve_with_state(listener: TcpListener, state: AppState) -> Result<(), RunError> {
    axum::serve(listener, router(state).into_make_service())
        .await
        .map_err(|err| RunError::Serve(err.to_string()))
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .compact()
        .try_init()
        .ok();
}

async fn capture(
    State(state): State<AppState>,
    payload: PostHogPayload<CaptureRequest>,
) -> Result<Json<PostHogResponse>, AppError> {
    let sent_at = payload.sent_at.clone();
    let events = payload
        .items
        .into_iter()
        .map(|item| PipelineEvent::from_capture(item).with_sent_at(sent_at.clone()))
        .collect();
    state.pipeline.send(events).await?;
    Ok(Json(PostHogResponse::success()))
}

/// Browser SDK sends events to /e/ with a different format:
/// - `token` instead of `api_key`
/// - `distinct_id` may be in `properties.$distinct_id` or `properties.distinct_id`
/// - `$set` and `$set_once` are top-level fields for identify events
#[derive(Debug, Deserialize)]
struct BrowserCaptureRequest {
    #[serde(default)]
    token: Option<String>,
    #[serde(default)]
    api_key: Option<String>,
    event: String,
    #[serde(default)]
    distinct_id: Option<String>,
    #[serde(default)]
    properties: Option<Value>,
    #[serde(default)]
    timestamp: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(rename = "$set")]
    #[serde(default)]
    set: Option<Value>,
    #[serde(rename = "$set_once")]
    #[serde(default)]
    set_once: Option<Value>,
}

async fn browser_capture(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<PostHogResponse>, AppError> {
    let sent_at = header_sent_at(&headers);

    let payload: BrowserCaptureRequest =
        serde_json::from_slice(&body).map_err(|e| AppError::InvalidPayload(e.to_string()))?;

    let api_key = payload.token.or(payload.api_key).or_else(|| header_api_key(&headers));

    // Extract distinct_id from payload or properties
    let distinct_id = payload.distinct_id.clone().or_else(|| {
        payload.properties.as_ref().and_then(|props| {
            props
                .get("$distinct_id")
                .or_else(|| props.get("distinct_id"))
                .and_then(Value::as_str)
                .map(String::from)
        })
    });

    let distinct_id =
        distinct_id.ok_or_else(|| AppError::InvalidPayload("missing distinct_id".into()))?;

    // Handle $identify events specially - use top-level $set as person_properties
    let event = if payload.event == "$identify" {
        let identify_req = IdentifyRequest {
            api_key,
            distinct_id,
            properties: payload.set.clone(),
            timestamp: payload.timestamp,
            context: None,
            extra: std::collections::HashMap::new(),
        };
        PipelineEvent::from_identify(identify_req)
    } else if payload.event == "$groupidentify" {
        // Handle group identify - extract group_type and group_key from $set
        let props = payload.properties.as_ref();
        let group_type = props
            .and_then(|p| p.get("$group_type").and_then(Value::as_str))
            .unwrap_or("unknown")
            .to_string();
        let group_key = props
            .and_then(|p| p.get("$group_key").and_then(Value::as_str))
            .unwrap_or("unknown")
            .to_string();
        let group_properties = props.and_then(|p| p.get("$group_set").cloned());

        let group_req = GroupIdentifyRequest {
            api_key,
            group_type,
            group_key,
            properties: group_properties,
            timestamp: payload.timestamp,
            extra: std::collections::HashMap::new(),
        };
        PipelineEvent::from_group_identify(group_req)
    } else {
        let capture_req = CaptureRequest {
            api_key,
            event: payload.event,
            distinct_id,
            properties: payload.properties,
            timestamp: payload.timestamp,
            context: None,
            extra: std::collections::HashMap::new(),
        };
        PipelineEvent::from_capture(capture_req)
    };

    let event = event.with_sent_at(sent_at);
    state.pipeline.send(vec![event]).await?;
    Ok(Json(PostHogResponse::success()))
}

async fn identify(
    State(state): State<AppState>,
    payload: PostHogPayload<IdentifyRequest>,
) -> Result<Json<PostHogResponse>, AppError> {
    let sent_at = payload.sent_at.clone();
    let events = payload
        .items
        .into_iter()
        .map(|item| PipelineEvent::from_identify(item).with_sent_at(sent_at.clone()))
        .collect();
    state.pipeline.send(events).await?;
    Ok(Json(PostHogResponse::success()))
}

async fn batch(
    State(state): State<AppState>,
    payload: PostHogBatchPayload,
) -> Result<Json<PostHogResponse>, AppError> {
    let sent_at = payload.batch.sent_at.clone();
    let shared_api_key = payload.batch.api_key.clone();
    let events = convert_batch(payload.batch, shared_api_key)
        .map_err(AppError::InvalidPayload)?
        .into_iter()
        .map(|event| event.with_sent_at(sent_at.clone()))
        .collect();
    state.pipeline.send(events).await?;
    Ok(Json(PostHogResponse::success()))
}

async fn groups(
    State(state): State<AppState>,
    payload: PostHogPayload<GroupIdentifyRequest>,
) -> Result<Json<PostHogResponse>, AppError> {
    let sent_at = payload.sent_at.clone();
    let events = payload
        .items
        .into_iter()
        .map(|item| PipelineEvent::from_group_identify(item).with_sent_at(sent_at.clone()))
        .collect();
    state.pipeline.send(events).await?;
    Ok(Json(PostHogResponse::success()))
}

async fn alias(
    State(state): State<AppState>,
    payload: PostHogPayload<AliasRequest>,
) -> Result<Json<PostHogResponse>, AppError> {
    let sent_at = payload.sent_at.clone();
    let events = payload
        .items
        .into_iter()
        .map(|item| PipelineEvent::from_alias(item).with_sent_at(sent_at.clone()))
        .collect();
    state.pipeline.send(events).await?;
    Ok(Json(PostHogResponse::success()))
}

async fn engage(
    State(state): State<AppState>,
    payload: PostHogPayload<EngageRequest>,
) -> Result<Json<PostHogResponse>, AppError> {
    let sent_at = payload.sent_at.clone();
    let events = payload
        .items
        .into_iter()
        .map(|item| PipelineEvent::from_engage(item).with_sent_at(sent_at.clone()))
        .collect();
    state.pipeline.send(events).await?;
    Ok(Json(PostHogResponse::success()))
}

#[derive(Default, Deserialize)]
struct DecideRequest {
    #[serde(default)]
    api_key: Option<String>,
    #[serde(default)]
    token: Option<String>,
}

async fn decide(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<DecideRequest>,
) -> impl IntoResponse {
    let header_key = headers
        .get("x-posthog-api-key")
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_string());

    let api_key = payload
        .api_key
        .or(payload.token)
        .or(header_key)
        .or(state.decide_api_token.clone());

    let mut response = DecideResponse::default();
    response.config.api_token = api_key;

    if let Some(endpoint) = state.session_recording_endpoint.clone() {
        response.session_recording.endpoint = Some(endpoint);
        response.session_recording.proxy = true;
    }

    Json(response)
}

async fn session_recording(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<PostHogResponse>, AppError> {
    let raw = body.to_vec();
    verify_signature(&headers, &raw, state.signing_secret.as_deref()).map_err(AppError::from)?;

    let sent_at = header_sent_at(&headers);
    let payload: Value =
        serde_json::from_slice(&raw).map_err(|err| AppError::InvalidPayload(err.to_string()))?;

    let api_key = header_api_key(&headers).or_else(|| {
        payload
            .get("token")
            .and_then(Value::as_str)
            .map(|s| s.to_string())
    });

    let distinct_id = payload
        .pointer("/data/metadata/distinct_id")
        .or_else(|| payload.get("distinct_id"))
        .and_then(Value::as_str)
        .unwrap_or("session-recording")
        .to_string();

    let event =
        PipelineEvent::from_session_recording(distinct_id, payload, api_key).with_sent_at(sent_at);

    state.pipeline.send(vec![event]).await?;
    Ok(Json(PostHogResponse::success()))
}

async fn health() -> impl IntoResponse {
    Json(json!({ "status": "ok" }))
}

#[derive(Debug, Error)]
pub enum RunError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Pipeline(#[from] PipelineError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("server error: {0}")]
    Serve(String),
}

fn convert_batch(
    batch: BatchRequest,
    shared_api_key: Option<String>,
) -> Result<Vec<PipelineEvent>, String> {
    batch
        .batch
        .into_iter()
        .map(|value| convert_batch_item(value, shared_api_key.as_ref()))
        .collect()
}

fn convert_batch_item(
    mut value: Value,
    shared_api_key: Option<&String>,
) -> Result<PipelineEvent, String> {
    let (event_field, type_field, has_alias_fields) = {
        let map = value
            .as_object_mut()
            .ok_or_else(|| "expected JSON object in batch payload".to_string())?;

        if map.get("api_key").is_none() {
            if let Some(api_key) = shared_api_key {
                map.insert("api_key".to_string(), Value::String(api_key.clone()));
            }
        }

        let event_field = map
            .get("event")
            .and_then(Value::as_str)
            .map(|s| s.to_string());
        let type_field = map
            .get("type")
            .and_then(Value::as_str)
            .map(|s| s.to_ascii_lowercase());
        let has_alias_fields = map.contains_key("alias") && map.contains_key("distinct_id");

        (event_field, type_field, has_alias_fields)
    };

    if matches!(type_field.as_deref(), Some("identify"))
        || matches!(event_field.as_deref(), Some("$identify"))
    {
        return serde_json::from_value::<IdentifyRequest>(value)
            .map(PipelineEvent::from_identify)
            .map_err(|err| format!("invalid identify event: {err}"));
    }

    if matches!(type_field.as_deref(), Some("groupidentify"))
        || matches!(type_field.as_deref(), Some("group_identify"))
        || matches!(event_field.as_deref(), Some("$groupidentify"))
    {
        return serde_json::from_value::<GroupIdentifyRequest>(value)
            .map(PipelineEvent::from_group_identify)
            .map_err(|err| format!("invalid group identify event: {err}"));
    }

    if matches!(type_field.as_deref(), Some("alias"))
        || matches!(event_field.as_deref(), Some("$create_alias"))
        || has_alias_fields
    {
        return serde_json::from_value::<AliasRequest>(value)
            .map(PipelineEvent::from_alias)
            .map_err(|err| format!("invalid alias event: {err}"));
    }

    if matches!(type_field.as_deref(), Some("engage")) {
        return serde_json::from_value::<EngageRequest>(value)
            .map(PipelineEvent::from_engage)
            .map_err(|err| format!("invalid engage event: {err}"));
    }

    serde_json::from_value::<CaptureRequest>(value)
        .map(PipelineEvent::from_capture)
        .map_err(|err| format!("invalid capture event: {err}"))
}
