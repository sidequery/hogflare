pub mod config;
pub mod extractors;
pub mod models;
pub mod pipeline;

use std::sync::Arc;

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use config::{Config, ConfigError};
use extractors::{PostHogBatchPayload, PostHogPayload};
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
struct AppState {
    pipeline: Arc<PipelineClient>,
    decide_api_token: Option<String>,
    session_recording_endpoint: Option<String>,
}

#[derive(Debug, Error)]
enum AppError {
    #[error(transparent)]
    Pipeline(#[from] PipelineError),
    #[error("invalid payload: {0}")]
    InvalidPayload(String),
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
        };

        let body = Json(ErrorResponse {
            status: 0,
            error: message,
        });

        (status, body).into_response()
    }
}

pub async fn run() -> Result<(), RunError> {
    dotenvy::dotenv().ok();
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

    let listener = TcpListener::bind(config.address).await?;
    info!(address = %config.address, "listening for requests");

    let state = build_state(
        Arc::new(pipeline),
        config.posthog_project_api_key.clone(),
        config.session_recording_endpoint.clone(),
    );

    serve_with_state(listener, state).await
}

pub fn build_router(pipeline: Arc<PipelineClient>) -> Router {
    build_router_with_options(pipeline, None, None)
}

pub fn build_router_with_options(
    pipeline: Arc<PipelineClient>,
    decide_api_token: Option<String>,
    session_recording_endpoint: Option<String>,
) -> Router {
    router(build_state(
        pipeline,
        decide_api_token,
        session_recording_endpoint,
    ))
}

pub async fn serve(listener: TcpListener, pipeline: Arc<PipelineClient>) -> Result<(), RunError> {
    serve_with_state(listener, build_state(pipeline, None, None)).await
}

pub async fn serve_with_options(
    listener: TcpListener,
    pipeline: Arc<PipelineClient>,
    decide_api_token: Option<String>,
    session_recording_endpoint: Option<String>,
) -> Result<(), RunError> {
    let state = build_state(pipeline, decide_api_token, session_recording_endpoint);
    serve_with_state(listener, state).await
}

fn router(state: AppState) -> Router {
    Router::new()
        .route("/capture", post(capture))
        .route("/identify", post(identify))
        .route("/batch", post(batch))
        .route("/groups", post(groups))
        .route("/alias", post(alias))
        .route("/engage", post(engage))
        .route("/decide", post(decide))
        .route("/s", post(session_recording))
        .route("/s/", post(session_recording))
        .route("/healthz", get(health))
        .with_state(state)
}

fn build_state(
    pipeline: Arc<PipelineClient>,
    decide_api_token: Option<String>,
    session_recording_endpoint: Option<String>,
) -> AppState {
    AppState {
        pipeline,
        decide_api_token,
        session_recording_endpoint,
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

async fn session_recording() -> impl IntoResponse {
    Json(PostHogResponse::success())
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
