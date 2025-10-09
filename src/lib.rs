pub mod config;
pub mod models;
pub mod pipeline;

use std::sync::Arc;

use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use config::{Config, ConfigError};
use models::{CaptureRequest, ErrorResponse, IdentifyRequest, PostHogResponse};
use pipeline::{PipelineClient, PipelineError, PipelineEvent};
use serde_json::json;
use thiserror::Error;
use tokio::net::TcpListener;
use tracing::{error, info};

#[derive(Clone)]
struct AppState {
    pipeline: Arc<PipelineClient>,
}

#[derive(Debug, Error)]
enum AppError {
    #[error(transparent)]
    Pipeline(#[from] PipelineError),
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        error!(%self, "request failed");
        let body = Json(ErrorResponse {
            status: 0,
            error: "internal server error".to_string(),
        });
        (StatusCode::INTERNAL_SERVER_ERROR, body).into_response()
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

    serve(listener, Arc::new(pipeline)).await
}

pub fn build_router(pipeline: Arc<PipelineClient>) -> Router {
    Router::new()
        .route("/capture", post(capture))
        .route("/identify", post(identify))
        .route("/healthz", get(health))
        .with_state(AppState { pipeline })
}

pub async fn serve(listener: TcpListener, pipeline: Arc<PipelineClient>) -> Result<(), RunError> {
    axum::serve(listener, build_router(pipeline).into_make_service())
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
    Json(payload): Json<CaptureRequest>,
) -> Result<Json<PostHogResponse>, AppError> {
    let event = PipelineEvent::from_capture(payload);
    state.pipeline.send(vec![event]).await?;
    Ok(Json(PostHogResponse::success()))
}

async fn identify(
    State(state): State<AppState>,
    Json(payload): Json<IdentifyRequest>,
) -> Result<Json<PostHogResponse>, AppError> {
    let event = PipelineEvent::from_identify(payload);
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
