pub mod config;
pub mod extractors;
pub mod feature_flags;
pub mod groups;
#[cfg(not(target_arch = "wasm32"))]
pub mod importer;
pub mod models;
pub mod persons;
pub mod pipeline;
pub mod product_analytics;
pub mod replay;
pub mod ui;

use std::{collections::HashMap, sync::Arc};

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{Html, IntoResponse},
    routing::{get, patch, post},
    Json, Router,
};
use chrono::Utc;
use config::{Config, ConfigError};
use extractors::{
    ApplyApiKey, PostHogBatchPayload, PostHogPayload, PostHogRawPayload, RequestEnrichment,
};
use feature_flags::{FeatureFlagContext, FeatureFlagStore};
use groups::{GroupError, GroupStore, GroupTypeMap, NoopGroupStore};
use models::{
    AliasRequest, BatchRequest, CaptureRequest, DecideResponse, EngageRequest, ErrorResponse,
    GroupIdentifyRequest, IdentifyRequest, PostHogResponse,
};
use persons::{
    alias_from_request, update_from_capture, update_from_engage, update_from_identify,
    NoopPersonStore, PersonAlias, PersonError, PersonStore, PersonUpdate,
};
use pipeline::{PersonPipelineRecord, PipelineClient, PipelineError, PipelineEvent};
use product_analytics::{ProductAnalyticsClient, ProductAnalyticsError, ProductAnalyticsQuery};
use replay::{
    ReplayClient, ReplayError, ReplayEventsQuery, ReplayFrictionQuery, ReplayFunnelQuery,
    ReplayPersonQuery, ReplaySessionEventsQuery, ReplaySessionsQuery,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use thiserror::Error;
#[cfg(not(target_arch = "wasm32"))]
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
#[cfg(not(target_arch = "wasm32"))]
use tracing::info;
#[cfg(not(target_arch = "wasm32"))]
use tracing::Level;
use tracing::{error, warn};

#[cfg(not(target_arch = "wasm32"))]
use tokio::net::TcpListener;

#[cfg(target_arch = "wasm32")]
use worker::{event, Context, Env, HttpRequest, Result as WorkerResult};

#[cfg(target_arch = "wasm32")]
use tower_service::Service;

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) pipeline: Arc<PipelineClient>,
    pub(crate) persons_pipeline: Option<Arc<PipelineClient>>,
    pub(crate) replay: Option<Arc<ReplayClient>>,
    pub(crate) analytics: Option<Arc<ProductAnalyticsClient>>,
    pub(crate) posthog_team_id: Option<i64>,
    pub(crate) decide_api_token: Option<String>,
    pub(crate) session_recording_endpoint: Option<String>,
    pub(crate) signing_secret: Option<String>,
    pub(crate) person_store: Arc<dyn PersonStore>,
    pub(crate) person_debug_token: Option<String>,
    pub(crate) group_store: Arc<dyn GroupStore>,
    pub(crate) group_type_map: GroupTypeMap,
    pub(crate) feature_flags: Arc<FeatureFlagStore>,
}

#[derive(Debug, Error)]
enum AppError {
    #[error(transparent)]
    Pipeline(#[from] PipelineError),
    #[error(transparent)]
    Person(#[from] PersonError),
    #[error(transparent)]
    Group(#[from] GroupError),
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
            AppError::Person(err) => {
                error!(error = %err, "person update failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "person update failed".to_string(),
                )
            }
            AppError::Group(err) => {
                error!(error = %err, "group update failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "group update failed".to_string(),
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

#[cfg(not(target_arch = "wasm32"))]
pub async fn run() -> Result<(), RunError> {
    // Load .env.local first, then .env as fallback
    dotenvy::from_filename(".env.local")
        .or_else(|_| dotenvy::dotenv())
        .ok();
    init_tracing();

    let config = Config::from_env()?;
    run_with_config(config).await
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn run_with_config(config: Config) -> Result<(), RunError> {
    let pipeline = PipelineClient::new(
        config.pipeline_endpoint.clone(),
        config.pipeline_auth_token.clone(),
        config.pipeline_timeout,
    )?;
    let persons_pipeline = config
        .persons_pipeline_endpoint
        .as_ref()
        .map(|endpoint| {
            PipelineClient::new(
                endpoint.clone(),
                config.persons_pipeline_auth_token.clone(),
                config.pipeline_timeout,
            )
            .map(Arc::new)
        })
        .transpose()?;
    let replay = config
        .replay
        .clone()
        .map(ReplayClient::new)
        .transpose()?
        .map(Arc::new);
    let analytics = config
        .analytics
        .clone()
        .map(ProductAnalyticsClient::new)
        .map(Arc::new);

    info!(
        endpoint = %config.pipeline_endpoint,
        auth_configured = config.pipeline_auth_token.is_some(),
        timeout_secs = config.pipeline_timeout.as_secs(),
        "pipeline client configured"
    );
    if let Some(endpoint) = config.persons_pipeline_endpoint.as_ref() {
        info!(
            endpoint = %endpoint,
            auth_configured = config.persons_pipeline_auth_token.is_some(),
            timeout_secs = config.pipeline_timeout.as_secs(),
            "persons pipeline client configured"
        );
    }

    let listener = TcpListener::bind(config.address).await?;
    info!(address = %config.address, "listening for requests");

    serve_with_state(
        listener,
        build_state(
            Arc::new(pipeline),
            persons_pipeline,
            replay,
            analytics,
            config.posthog_team_id,
            Arc::new(NoopGroupStore),
            GroupTypeMap::new(config.posthog_group_types.clone()),
            config.posthog_project_api_key.clone(),
            config.session_recording_endpoint.clone(),
            config.posthog_signing_secret.clone(),
            config.person_debug_token.clone(),
            Arc::new(config.feature_flags),
            Arc::new(persons::MemoryPersonStore::new(config.posthog_team_id)),
        ),
    )
    .await
}

#[cfg(target_arch = "wasm32")]
#[event(fetch)]
pub async fn fetch(
    req: HttpRequest,
    env: Env,
    _ctx: Context,
) -> WorkerResult<http::Response<axum::body::Body>> {
    let config = match Config::from_worker_env(&env) {
        Ok(config) => config,
        Err(err) => {
            let body = Json(ErrorResponse {
                status: 0,
                error: err.to_string(),
            });
            return Ok((StatusCode::INTERNAL_SERVER_ERROR, body).into_response());
        }
    };

    let pipeline = match PipelineClient::new(
        config.pipeline_endpoint.clone(),
        config.pipeline_auth_token.clone(),
        config.pipeline_timeout,
    ) {
        Ok(client) => client,
        Err(err) => {
            error!(error = %err, "failed to create pipeline client");
            let body = Json(ErrorResponse {
                status: 0,
                error: err.to_string(),
            });
            return Ok((StatusCode::INTERNAL_SERVER_ERROR, body).into_response());
        }
    };
    let persons_pipeline = match config.persons_pipeline_endpoint.as_ref() {
        Some(endpoint) => match PipelineClient::new(
            endpoint.clone(),
            config.persons_pipeline_auth_token.clone(),
            config.pipeline_timeout,
        ) {
            Ok(client) => Some(Arc::new(client)),
            Err(err) => {
                error!(error = %err, "failed to create persons pipeline client");
                let body = Json(ErrorResponse {
                    status: 0,
                    error: err.to_string(),
                });
                return Ok((StatusCode::INTERNAL_SERVER_ERROR, body).into_response());
            }
        },
        None => None,
    };

    let person_store: Arc<dyn PersonStore> = persons::store_from_env(&env, config.posthog_team_id);
    let group_store: Arc<dyn GroupStore> = groups::store_from_env(&env);
    let group_type_map = GroupTypeMap::new(config.posthog_group_types.clone());
    let feature_flags = Arc::new(config.feature_flags);
    let replay = match config.replay.clone() {
        Some(config) => match ReplayClient::new(config) {
            Ok(client) => Some(Arc::new(client)),
            Err(err) => {
                error!(error = %err, "failed to create replay client");
                let body = Json(ErrorResponse {
                    status: 0,
                    error: err.to_string(),
                });
                return Ok((StatusCode::INTERNAL_SERVER_ERROR, body).into_response());
            }
        },
        None => None,
    };
    let analytics = config
        .analytics
        .clone()
        .map(ProductAnalyticsClient::new)
        .map(Arc::new);

    let mut router = router(build_state(
        Arc::new(pipeline),
        persons_pipeline,
        replay,
        analytics,
        config.posthog_team_id,
        group_store,
        group_type_map,
        config.posthog_project_api_key.clone(),
        config.session_recording_endpoint.clone(),
        config.posthog_signing_secret.clone(),
        config.person_debug_token.clone(),
        feature_flags,
        person_store,
    ));

    Ok(router.call(req).await?)
}

pub fn build_router(pipeline: Arc<PipelineClient>) -> Router {
    build_router_with_options(
        pipeline,
        None,
        Arc::new(NoopGroupStore),
        GroupTypeMap::default(),
        None,
        None,
        None,
        None,
        Arc::new(FeatureFlagStore::empty()),
        Arc::new(NoopPersonStore),
    )
}

pub fn build_router_with_options(
    pipeline: Arc<PipelineClient>,
    posthog_team_id: Option<i64>,
    group_store: Arc<dyn GroupStore>,
    group_type_map: GroupTypeMap,
    decide_api_token: Option<String>,
    session_recording_endpoint: Option<String>,
    signing_secret: Option<String>,
    person_debug_token: Option<String>,
    feature_flags: Arc<FeatureFlagStore>,
    person_store: Arc<dyn PersonStore>,
) -> Router {
    build_router_with_person_pipeline(
        pipeline,
        None,
        posthog_team_id,
        group_store,
        group_type_map,
        decide_api_token,
        session_recording_endpoint,
        signing_secret,
        person_debug_token,
        feature_flags,
        person_store,
    )
}

pub fn build_router_with_person_pipeline(
    pipeline: Arc<PipelineClient>,
    persons_pipeline: Option<Arc<PipelineClient>>,
    posthog_team_id: Option<i64>,
    group_store: Arc<dyn GroupStore>,
    group_type_map: GroupTypeMap,
    decide_api_token: Option<String>,
    session_recording_endpoint: Option<String>,
    signing_secret: Option<String>,
    person_debug_token: Option<String>,
    feature_flags: Arc<FeatureFlagStore>,
    person_store: Arc<dyn PersonStore>,
) -> Router {
    build_router_with_person_pipeline_and_replay(
        pipeline,
        persons_pipeline,
        None,
        posthog_team_id,
        group_store,
        group_type_map,
        decide_api_token,
        session_recording_endpoint,
        signing_secret,
        person_debug_token,
        feature_flags,
        person_store,
    )
}

pub fn build_router_with_person_pipeline_and_replay(
    pipeline: Arc<PipelineClient>,
    persons_pipeline: Option<Arc<PipelineClient>>,
    replay: Option<Arc<ReplayClient>>,
    posthog_team_id: Option<i64>,
    group_store: Arc<dyn GroupStore>,
    group_type_map: GroupTypeMap,
    decide_api_token: Option<String>,
    session_recording_endpoint: Option<String>,
    signing_secret: Option<String>,
    person_debug_token: Option<String>,
    feature_flags: Arc<FeatureFlagStore>,
    person_store: Arc<dyn PersonStore>,
) -> Router {
    router(build_state(
        pipeline,
        persons_pipeline,
        replay,
        None,
        posthog_team_id,
        group_store,
        group_type_map,
        decide_api_token,
        session_recording_endpoint,
        signing_secret,
        person_debug_token,
        feature_flags,
        person_store,
    ))
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn serve(listener: TcpListener, pipeline: Arc<PipelineClient>) -> Result<(), RunError> {
    serve_with_state(
        listener,
        build_state(
            pipeline,
            None,
            None,
            None,
            None,
            Arc::new(NoopGroupStore),
            GroupTypeMap::default(),
            None,
            None,
            None,
            None,
            Arc::new(FeatureFlagStore::empty()),
            Arc::new(persons::MemoryPersonStore::new(None)),
        ),
    )
    .await
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn serve_with_options(
    listener: TcpListener,
    pipeline: Arc<PipelineClient>,
    posthog_team_id: Option<i64>,
    group_store: Arc<dyn GroupStore>,
    group_type_map: GroupTypeMap,
    decide_api_token: Option<String>,
    session_recording_endpoint: Option<String>,
    signing_secret: Option<String>,
    person_debug_token: Option<String>,
    feature_flags: Arc<FeatureFlagStore>,
) -> Result<(), RunError> {
    serve_with_person_pipeline(
        listener,
        pipeline,
        None,
        posthog_team_id,
        group_store,
        group_type_map,
        decide_api_token,
        session_recording_endpoint,
        signing_secret,
        person_debug_token,
        feature_flags,
    )
    .await
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn serve_with_person_pipeline(
    listener: TcpListener,
    pipeline: Arc<PipelineClient>,
    persons_pipeline: Option<Arc<PipelineClient>>,
    posthog_team_id: Option<i64>,
    group_store: Arc<dyn GroupStore>,
    group_type_map: GroupTypeMap,
    decide_api_token: Option<String>,
    session_recording_endpoint: Option<String>,
    signing_secret: Option<String>,
    person_debug_token: Option<String>,
    feature_flags: Arc<FeatureFlagStore>,
) -> Result<(), RunError> {
    serve_with_person_pipeline_and_replay(
        listener,
        pipeline,
        persons_pipeline,
        None,
        posthog_team_id,
        group_store,
        group_type_map,
        decide_api_token,
        session_recording_endpoint,
        signing_secret,
        person_debug_token,
        feature_flags,
    )
    .await
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn serve_with_person_pipeline_and_replay(
    listener: TcpListener,
    pipeline: Arc<PipelineClient>,
    persons_pipeline: Option<Arc<PipelineClient>>,
    replay: Option<Arc<ReplayClient>>,
    posthog_team_id: Option<i64>,
    group_store: Arc<dyn GroupStore>,
    group_type_map: GroupTypeMap,
    decide_api_token: Option<String>,
    session_recording_endpoint: Option<String>,
    signing_secret: Option<String>,
    person_debug_token: Option<String>,
    feature_flags: Arc<FeatureFlagStore>,
) -> Result<(), RunError> {
    let state = build_state(
        pipeline,
        persons_pipeline,
        replay,
        None,
        posthog_team_id,
        group_store,
        group_type_map,
        decide_api_token,
        session_recording_endpoint,
        signing_secret,
        person_debug_token,
        feature_flags,
        Arc::new(persons::MemoryPersonStore::new(posthog_team_id)),
    );
    serve_with_state(listener, state).await
}

fn router(state: AppState) -> Router {
    let router = Router::new()
        .route("/", get(app_ui))
        .route("/capture", post(capture))
        .route("/e", post(browser_capture))
        .route("/e/", post(browser_capture))
        .route("/i/v0/e", post(browser_capture))
        .route("/i/v0/e/", post(browser_capture))
        .route("/identify", post(identify))
        .route("/batch", post(batch))
        .route("/batch/", post(batch))
        .route("/groups", post(groups))
        .route("/alias", post(alias))
        .route("/engage", post(engage))
        .route("/decide", post(decide))
        .route("/flags", post(flags))
        .route("/flags/", post(flags))
        .route("/array/:token/config", get(array_config))
        .route("/array/:token/config.js", get(array_config_js))
        .route(
            "/static/exception-autocapture.js",
            get(exception_autocapture_js),
        )
        .route(
            "/static/:version/exception-autocapture.js",
            get(exception_autocapture_js),
        )
        .route("/s", post(session_recording))
        .route("/s/", post(session_recording))
        .route("/analytics", get(app_ui))
        .route("/analytics/", get(app_ui))
        .route("/analytics/api/charts", get(product_analytics))
        .route(
            "/errors/api/issues/:fingerprint/status",
            patch(update_error_issue_status).post(update_error_issue_status),
        )
        .route("/replay", get(app_ui))
        .route("/replay/", get(app_ui))
        .route("/replay/api/sessions", get(replay_sessions))
        .route("/replay/api/events", get(replay_events))
        .route("/replay/api/funnels", get(replay_funnels))
        .route("/replay/api/friction", get(replay_friction))
        .route("/replay/api/person", get(replay_person))
        .route(
            "/replay/api/sessions/:session_id",
            get(replay_session_events),
        )
        .route("/__debug/person/:id", get(debug_person))
        .route("/healthz", get(health))
        .with_state(state);

    #[cfg(not(target_arch = "wasm32"))]
    let router = router.layer(
        TraceLayer::new_for_http()
            .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
            .on_response(DefaultOnResponse::new().level(Level::INFO)),
    );

    router
}

fn build_state(
    pipeline: Arc<PipelineClient>,
    persons_pipeline: Option<Arc<PipelineClient>>,
    replay: Option<Arc<ReplayClient>>,
    analytics: Option<Arc<ProductAnalyticsClient>>,
    posthog_team_id: Option<i64>,
    group_store: Arc<dyn GroupStore>,
    group_type_map: GroupTypeMap,
    decide_api_token: Option<String>,
    session_recording_endpoint: Option<String>,
    signing_secret: Option<String>,
    person_debug_token: Option<String>,
    feature_flags: Arc<FeatureFlagStore>,
    person_store: Arc<dyn PersonStore>,
) -> AppState {
    AppState {
        pipeline,
        persons_pipeline,
        replay,
        analytics,
        posthog_team_id,
        group_store,
        group_type_map,
        decide_api_token,
        session_recording_endpoint,
        signing_secret,
        person_store,
        person_debug_token,
        feature_flags,
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn serve_with_state(listener: TcpListener, state: AppState) -> Result<(), RunError> {
    axum::serve(listener, router(state).into_make_service())
        .await
        .map_err(|err| RunError::Serve(err.to_string()))
}

#[cfg(not(target_arch = "wasm32"))]
fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .compact()
        .try_init()
        .ok();
}

#[cfg(target_arch = "wasm32")]
#[allow(dead_code)]
fn init_tracing() {}

#[cfg_attr(target_arch = "wasm32", worker::send)]
async fn capture(
    State(state): State<AppState>,
    enrichment: RequestEnrichment,
    payload: PostHogPayload<CaptureRequest>,
) -> Result<Json<PostHogResponse>, AppError> {
    let sent_at = payload.sent_at.clone();
    let enrichment = enrichment.properties();
    let mut events = Vec::new();
    let mut person_records = Vec::new();

    for item in payload.items {
        if item.event == "$groupidentify" {
            let group_req = group_identify_from_capture(item)?;
            let snapshot = state
                .group_store
                .apply_update(group_update_from_identify(&group_req))
                .await?;
            let (group_slots, group_properties) =
                group_fields_from_snapshot(&state.group_type_map, snapshot);
            events.push(
                PipelineEvent::from_group_identify(group_req)
                    .with_team_id(state.posthog_team_id)
                    .with_groups(group_slots, group_properties)
                    .with_sent_at(sent_at.clone())
                    .with_enrichment(enrichment),
            );
            continue;
        }

        let update = update_from_capture(&item);
        let snapshot = match update {
            Some(update) => apply_person_update(&state, update).await?,
            None => ensure_person_snapshot(&state, &item.distinct_id).await?,
        };

        let groups = extract_groups(&item.properties);
        let group_set = if let Some(Value::Object(props)) = item.properties.as_ref() {
            extract_group_set(props.get("$group_set"))
        } else {
            serde_json::Map::new()
        };

        if let Some(groups_map) = groups.as_ref() {
            for (group_type, props) in &group_set {
                let Some(group_key) = groups_map.get(group_type).and_then(Value::as_str) else {
                    continue;
                };
                let Some(props_map) = props.as_object() else {
                    continue;
                };
                if props_map.is_empty() {
                    continue;
                }
                state
                    .group_store
                    .apply_update(groups::GroupUpdate {
                        group_type: group_type.clone(),
                        group_key: group_key.to_string(),
                        properties: props_map.clone(),
                    })
                    .await?;
            }
        }

        let group_slots = groups
            .as_ref()
            .map(|map| group_slots_from_map(&state.group_type_map, map))
            .unwrap_or([None, None, None, None, None]);
        let group_properties = if let Some(groups_map) = groups.as_ref() {
            hydrate_group_properties(&state, groups_map).await?
        } else {
            None
        };

        let (person_id, person_created_at, person_properties) = person_fields(&snapshot);
        let event = PipelineEvent::from_capture(item)
            .with_team_id(state.posthog_team_id)
            .with_person(person_id, person_created_at, person_properties)
            .with_groups(group_slots, group_properties)
            .with_sent_at(sent_at.clone())
            .with_enrichment(enrichment);
        push_person_record(&mut person_records, &snapshot, "capture", &event);
        events.push(event);
    }

    send_person_records(&state, person_records).await?;
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
    #[serde(default)]
    #[serde(flatten)]
    extra: std::collections::HashMap<String, Value>,
}

impl ApplyApiKey for BrowserCaptureRequest {
    fn ensure_api_key(&mut self, api_key: &str) {
        if self.token.is_none() && self.api_key.is_none() {
            self.api_key = Some(api_key.to_string());
        }
    }
}

#[cfg_attr(target_arch = "wasm32", worker::send)]
async fn browser_capture(
    State(state): State<AppState>,
    enrichment: RequestEnrichment,
    payload: PostHogPayload<BrowserCaptureRequest>,
) -> Result<Json<PostHogResponse>, AppError> {
    let sent_at = payload.sent_at.clone();
    let enrichment = enrichment.properties();
    let mut events = Vec::new();
    let mut person_records = Vec::new();

    for payload in payload.items {
        let api_key = payload
            .token
            .clone()
            .or(payload.api_key.clone())
            .or_else(|| api_key_from_browser_properties(payload.properties.as_ref()));

        let distinct_id = browser_distinct_id(&payload)
            .ok_or_else(|| AppError::InvalidPayload("missing distinct_id".into()))?;

        let event = if payload.event == "$identify" {
            let identify_req = browser_identify_request(payload, api_key, distinct_id);
            if let Some(anon) = anon_distinct_id_from_identify(&identify_req) {
                if anon != identify_req.distinct_id {
                    state
                        .person_store
                        .apply_alias(PersonAlias {
                            distinct_id: identify_req.distinct_id.clone(),
                            alias: anon,
                        })
                        .await?;
                }
            }
            PipelineEvent::from_identify(identify_req)
        } else if payload.event == "$groupidentify" {
            let group_req = browser_group_identify_request(payload, api_key)?;
            let group_update = group_update_from_identify(&group_req);
            state.group_store.apply_update(group_update).await?;
            PipelineEvent::from_group_identify(group_req)
        } else {
            let capture_req = CaptureRequest {
                api_key,
                event: payload.event,
                distinct_id,
                properties: payload.properties,
                timestamp: payload.timestamp,
                context: None,
                extra: payload.extra,
            };
            PipelineEvent::from_capture(capture_req)
        };

        let mut group_slots = [None, None, None, None, None];
        let mut group_properties = None;

        if event.event == "$groupidentify" {
            if let Some(group_type) = event.extra.get("group_type").and_then(Value::as_str) {
                if let Some(group_key) = event.extra.get("group_key").and_then(Value::as_str) {
                    if let Some(index) = state.group_type_map.index_for(group_type) {
                        group_slots[index] = Some(group_key.to_string());
                    }
                    let snapshot = state
                        .group_store
                        .get_snapshot(group_type, group_key)
                        .await?;
                    if let Some(record) = snapshot.record {
                        let mut props = serde_json::Map::new();
                        props.insert(record.group_type.clone(), Value::Object(record.properties));
                        group_properties = Some(Value::Object(props));
                    }
                }
            }
        } else {
            let groups = extract_groups(&event.properties);
            let group_set = if let Some(Value::Object(props)) = event.properties.as_ref() {
                extract_group_set(props.get("$group_set"))
            } else {
                serde_json::Map::new()
            };

            if let Some(groups_map) = groups.as_ref() {
                for (group_type, props) in &group_set {
                    let Some(group_key) = groups_map.get(group_type).and_then(Value::as_str) else {
                        continue;
                    };
                    let Some(props_map) = props.as_object() else {
                        continue;
                    };
                    if props_map.is_empty() {
                        continue;
                    }
                    state
                        .group_store
                        .apply_update(groups::GroupUpdate {
                            group_type: group_type.clone(),
                            group_key: group_key.to_string(),
                            properties: props_map.clone(),
                        })
                        .await?;
                }

                group_slots = group_slots_from_map(&state.group_type_map, groups_map);
                group_properties = hydrate_group_properties(&state, groups_map).await?;
            }
        }

        let snapshot = if event.event == "$groupidentify" {
            None
        } else {
            let update = if event.event == "$identify" {
                update_from_identify(&IdentifyRequest {
                    api_key: event.api_key.clone(),
                    distinct_id: event.distinct_id.clone(),
                    anon_distinct_id: None,
                    properties: event.person_properties.clone(),
                    timestamp: event.timestamp,
                    context: None,
                    extra: event.extra.clone(),
                })
            } else {
                update_from_capture(&CaptureRequest {
                    api_key: event.api_key.clone(),
                    event: event.event.clone(),
                    distinct_id: event.distinct_id.clone(),
                    properties: event.properties.clone(),
                    timestamp: event.timestamp,
                    context: None,
                    extra: event.extra.clone(),
                })
            };

            Some(match update {
                Some(update) => apply_person_update(&state, update).await?,
                None => ensure_person_snapshot(&state, &event.distinct_id).await?,
            })
        };

        let (person_id, person_created_at, person_properties) = snapshot
            .as_ref()
            .map(person_fields)
            .unwrap_or((None, None, None));

        let event = event
            .with_team_id(state.posthog_team_id)
            .with_person(person_id, person_created_at, person_properties)
            .with_groups(group_slots, group_properties)
            .with_sent_at(sent_at.clone())
            .with_enrichment(enrichment);
        if let Some(snapshot) = snapshot.as_ref() {
            let operation = if event.event == "$identify" {
                "identify"
            } else {
                "capture"
            };
            push_person_record(&mut person_records, snapshot, operation, &event);
        }
        events.push(event);
    }

    send_person_records(&state, person_records).await?;
    state.pipeline.send(events).await?;
    Ok(Json(PostHogResponse::success()))
}

#[cfg_attr(target_arch = "wasm32", worker::send)]
async fn identify(
    State(state): State<AppState>,
    enrichment: RequestEnrichment,
    payload: PostHogPayload<IdentifyRequest>,
) -> Result<Json<PostHogResponse>, AppError> {
    let sent_at = payload.sent_at.clone();
    let enrichment = enrichment.properties();
    let mut events = Vec::new();
    let mut person_records = Vec::new();

    for item in payload.items {
        if let Some(anon) = anon_distinct_id_from_identify(&item) {
            if anon != item.distinct_id {
                state
                    .person_store
                    .apply_alias(PersonAlias {
                        distinct_id: item.distinct_id.clone(),
                        alias: anon,
                    })
                    .await?;
            }
        }

        let update = update_from_identify(&item);
        let snapshot = match update {
            Some(update) => apply_person_update(&state, update).await?,
            None => ensure_person_snapshot(&state, &item.distinct_id).await?,
        };

        let groups = extract_groups(&item.properties);
        let group_slots = groups
            .as_ref()
            .map(|map| group_slots_from_map(&state.group_type_map, map))
            .unwrap_or([None, None, None, None, None]);
        let group_properties = if let Some(groups_map) = groups.as_ref() {
            hydrate_group_properties(&state, groups_map).await?
        } else {
            None
        };

        let (person_id, person_created_at, person_properties) = person_fields(&snapshot);
        let event = PipelineEvent::from_identify(item)
            .with_team_id(state.posthog_team_id)
            .with_person(person_id, person_created_at, person_properties)
            .with_groups(group_slots, group_properties)
            .with_sent_at(sent_at.clone())
            .with_enrichment(enrichment);
        push_person_record(&mut person_records, &snapshot, "identify", &event);
        events.push(event);
    }

    send_person_records(&state, person_records).await?;
    state.pipeline.send(events).await?;
    Ok(Json(PostHogResponse::success()))
}

#[cfg_attr(target_arch = "wasm32", worker::send)]
async fn batch(
    State(state): State<AppState>,
    enrichment: RequestEnrichment,
    payload: PostHogBatchPayload,
) -> Result<Json<PostHogResponse>, AppError> {
    let sent_at = payload.batch.sent_at.clone();
    let shared_api_key = payload.batch.api_key.clone();
    let enrichment = enrichment.properties();
    let items = convert_batch(payload.batch, shared_api_key).map_err(AppError::InvalidPayload)?;

    let mut events = Vec::new();
    let mut person_records = Vec::new();

    for item in items {
        let operation = item.kind.person_operation();
        if let Some(alias) = item.alias {
            let snapshot = state.person_store.apply_alias(alias).await?;
            let (person_id, person_created_at, person_properties) = person_fields(&snapshot);
            let event = item
                .event
                .with_team_id(state.posthog_team_id)
                .with_person(person_id, person_created_at, person_properties)
                .with_groups([None, None, None, None, None], None)
                .with_sent_at(sent_at.clone())
                .with_enrichment(enrichment);
            push_person_record(&mut person_records, &snapshot, operation, &event);
            events.push(event);
            continue;
        }

        if let Some(group_update) = item.group_update {
            let snapshot = state.group_store.apply_update(group_update).await?;
            let (group_slots, group_properties) =
                group_fields_from_snapshot(&state.group_type_map, snapshot);

            let event = item
                .event
                .with_team_id(state.posthog_team_id)
                .with_groups(group_slots, group_properties)
                .with_sent_at(sent_at.clone())
                .with_enrichment(enrichment);
            events.push(event);
            continue;
        }

        if let Some(anon) = item.anon_distinct_id.clone() {
            if anon != item.event.distinct_id {
                state
                    .person_store
                    .apply_alias(PersonAlias {
                        distinct_id: item.event.distinct_id.clone(),
                        alias: anon,
                    })
                    .await?;
            }
        }

        if let Some(groups_map) = item.groups.as_ref() {
            for (group_type, props) in &item.group_set {
                let Some(group_key) = groups_map.get(group_type).and_then(Value::as_str) else {
                    continue;
                };
                let Some(props_map) = props.as_object() else {
                    continue;
                };
                if props_map.is_empty() {
                    continue;
                }
                state
                    .group_store
                    .apply_update(groups::GroupUpdate {
                        group_type: group_type.clone(),
                        group_key: group_key.to_string(),
                        properties: props_map.clone(),
                    })
                    .await?;
            }
        }

        let snapshot = match item.person_update {
            Some(update) => apply_person_update(&state, update).await?,
            None => ensure_person_snapshot(&state, &item.event.distinct_id).await?,
        };

        let group_slots = item
            .groups
            .as_ref()
            .map(|map| group_slots_from_map(&state.group_type_map, map))
            .unwrap_or([None, None, None, None, None]);
        let group_properties = if let Some(groups_map) = item.groups.as_ref() {
            hydrate_group_properties(&state, groups_map).await?
        } else {
            None
        };

        let (person_id, person_created_at, person_properties) = person_fields(&snapshot);
        let event = item
            .event
            .with_team_id(state.posthog_team_id)
            .with_person(person_id, person_created_at, person_properties)
            .with_groups(group_slots, group_properties)
            .with_sent_at(sent_at.clone())
            .with_enrichment(enrichment);
        push_person_record(&mut person_records, &snapshot, operation, &event);
        events.push(event);
    }

    send_person_records(&state, person_records).await?;
    state.pipeline.send(events).await?;
    Ok(Json(PostHogResponse::success()))
}

#[cfg_attr(target_arch = "wasm32", worker::send)]
async fn groups(
    State(state): State<AppState>,
    enrichment: RequestEnrichment,
    payload: PostHogPayload<GroupIdentifyRequest>,
) -> Result<Json<PostHogResponse>, AppError> {
    let sent_at = payload.sent_at.clone();
    let enrichment = enrichment.properties();
    let mut events = Vec::new();

    for item in payload.items {
        let item = normalize_group_identify_request(item)?;
        let snapshot = state
            .group_store
            .apply_update(group_update_from_identify(&item))
            .await?;
        let (group_slots, group_properties) =
            group_fields_from_snapshot(&state.group_type_map, snapshot);

        events.push(
            PipelineEvent::from_group_identify(item)
                .with_team_id(state.posthog_team_id)
                .with_groups(group_slots, group_properties)
                .with_sent_at(sent_at.clone())
                .with_enrichment(enrichment),
        );
    }
    state.pipeline.send(events).await?;
    Ok(Json(PostHogResponse::success()))
}

#[cfg_attr(target_arch = "wasm32", worker::send)]
async fn alias(
    State(state): State<AppState>,
    enrichment: RequestEnrichment,
    payload: PostHogPayload<AliasRequest>,
) -> Result<Json<PostHogResponse>, AppError> {
    let sent_at = payload.sent_at.clone();
    let enrichment = enrichment.properties();
    let mut events = Vec::new();
    let mut person_records = Vec::new();

    for item in payload.items {
        let snapshot = state
            .person_store
            .apply_alias(alias_from_request(&item))
            .await?;
        let (person_id, person_created_at, person_properties) = person_fields(&snapshot);
        let event = PipelineEvent::from_alias(item)
            .with_team_id(state.posthog_team_id)
            .with_person(person_id, person_created_at, person_properties)
            .with_groups([None, None, None, None, None], None)
            .with_sent_at(sent_at.clone())
            .with_enrichment(enrichment);
        push_person_record(&mut person_records, &snapshot, "alias", &event);
        events.push(event);
    }

    send_person_records(&state, person_records).await?;
    state.pipeline.send(events).await?;
    Ok(Json(PostHogResponse::success()))
}

#[cfg_attr(target_arch = "wasm32", worker::send)]
async fn engage(
    State(state): State<AppState>,
    enrichment: RequestEnrichment,
    payload: PostHogPayload<EngageRequest>,
) -> Result<Json<PostHogResponse>, AppError> {
    let sent_at = payload.sent_at.clone();
    let enrichment = enrichment.properties();
    let mut events = Vec::new();
    let mut person_records = Vec::new();

    for item in payload.items {
        let update = update_from_engage(&item);
        let snapshot = match update {
            Some(update) => apply_person_update(&state, update).await?,
            None => ensure_person_snapshot(&state, &item.distinct_id).await?,
        };

        let groups = item
            .extra
            .get("$groups")
            .and_then(|value| value.as_object())
            .cloned();
        let group_set = extract_group_set(item.group_set.as_ref());

        if let Some(groups_map) = groups.as_ref() {
            for (group_type, props) in &group_set {
                let Some(group_key) = groups_map.get(group_type).and_then(Value::as_str) else {
                    continue;
                };
                let Some(props_map) = props.as_object() else {
                    continue;
                };
                if props_map.is_empty() {
                    continue;
                }
                state
                    .group_store
                    .apply_update(groups::GroupUpdate {
                        group_type: group_type.clone(),
                        group_key: group_key.to_string(),
                        properties: props_map.clone(),
                    })
                    .await?;
            }
        }

        let group_slots = groups
            .as_ref()
            .map(|map| group_slots_from_map(&state.group_type_map, map))
            .unwrap_or([None, None, None, None, None]);
        let group_properties = if let Some(groups_map) = groups.as_ref() {
            hydrate_group_properties(&state, groups_map).await?
        } else {
            None
        };

        let (person_id, person_created_at, person_properties) = person_fields(&snapshot);
        let event = PipelineEvent::from_engage(item)
            .with_team_id(state.posthog_team_id)
            .with_person(person_id, person_created_at, person_properties)
            .with_groups(group_slots, group_properties)
            .with_sent_at(sent_at.clone())
            .with_enrichment(enrichment);
        push_person_record(&mut person_records, &snapshot, "engage", &event);
        events.push(event);
    }

    send_person_records(&state, person_records).await?;
    state.pipeline.send(events).await?;
    Ok(Json(PostHogResponse::success()))
}

#[derive(Default, Deserialize)]
struct DecideRequest {
    #[serde(default)]
    api_key: Option<String>,
    #[serde(default)]
    token: Option<String>,
    #[serde(default)]
    distinct_id: Option<String>,
    #[serde(default)]
    groups: Option<std::collections::HashMap<String, String>>,
    #[serde(default)]
    person_properties: Option<std::collections::HashMap<String, Value>>,
    #[serde(default, rename = "group_properties")]
    group_properties:
        Option<std::collections::HashMap<String, std::collections::HashMap<String, Value>>>,
    #[serde(default)]
    disable_flags: Option<bool>,
    #[serde(default)]
    flag_keys_to_evaluate: Option<Vec<String>>,
    #[serde(default)]
    evaluation_environments: Option<Vec<String>>,
    #[serde(default)]
    evaluation_contexts: Option<Vec<String>>,
}

impl ApplyApiKey for DecideRequest {
    fn ensure_api_key(&mut self, api_key: &str) {
        if self.api_key.is_none() && self.token.is_none() {
            self.api_key = Some(api_key.to_string());
        }
    }
}

#[derive(Default, Deserialize)]
struct FlagsQuery {
    #[serde(default)]
    v: Option<u8>,
    #[serde(default)]
    config: Option<bool>,
}

#[cfg_attr(target_arch = "wasm32", worker::send)]
async fn decide(
    State(state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<FlagsQuery>,
    payload: PostHogPayload<DecideRequest>,
) -> Result<Json<DecideResponse>, AppError> {
    let payload = payload.items.into_iter().next().unwrap_or_default();

    let api_key = payload
        .api_key
        .clone()
        .or(payload.token.clone())
        .or(state.decide_api_token.clone());

    let version = query.v.unwrap_or(2);
    let flags = evaluate_feature_flags(&state, &payload).await?;
    let (feature_flags, feature_flag_payloads) = flags.to_maps(version);

    let mut response = DecideResponse::default();
    response.config.api_token = api_key;
    response.feature_flags = feature_flags;
    response.feature_flag_payloads = feature_flag_payloads;
    response.session_recording = decide_session_recording_config(&state);

    Ok(Json(response))
}

#[derive(Serialize)]
struct FlagsResponse {
    #[serde(rename = "featureFlags")]
    feature_flags: std::collections::HashMap<String, Value>,
    #[serde(rename = "featureFlagPayloads")]
    feature_flag_payloads: std::collections::HashMap<String, Value>,
    #[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]
    flags: std::collections::HashMap<String, Value>,
    #[serde(rename = "errorsWhileComputingFlags")]
    errors_while_computing_flags: bool,
    #[serde(rename = "requestId")]
    request_id: String,
    #[serde(rename = "evaluatedAt")]
    evaluated_at: i64,
    #[serde(rename = "sessionRecording", skip_serializing_if = "Option::is_none")]
    session_recording: Option<Value>,
    #[serde(rename = "supportedCompression", skip_serializing_if = "Vec::is_empty")]
    supported_compression: Vec<String>,
}

#[cfg_attr(target_arch = "wasm32", worker::send)]
async fn flags(
    State(state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<FlagsQuery>,
    payload: PostHogPayload<DecideRequest>,
) -> Result<impl IntoResponse, AppError> {
    let payload = payload.items.into_iter().next().unwrap_or_default();
    let version = query.v.unwrap_or(2);
    let flags = evaluate_feature_flags(&state, &payload).await?;
    let (feature_flags, feature_flag_payloads) = flags.to_maps(version);
    let flag_details = flags.to_flag_details(version);
    let include_config = query.config.unwrap_or(false);
    let mut session_recording = None;
    let mut supported_compression = Vec::new();
    let request_id = flags.request_id();
    let evaluated_at = Utc::now().timestamp_millis();

    if include_config {
        session_recording = Some(decide_session_recording_config(&state));
        supported_compression = vec!["gzip".to_string(), "gzip-js".to_string()];
    }

    Ok(Json(FlagsResponse {
        feature_flags,
        feature_flag_payloads,
        flags: flag_details,
        errors_while_computing_flags: false,
        request_id,
        evaluated_at,
        session_recording,
        supported_compression,
    })
    .into_response())
}

fn decide_session_recording_config(state: &AppState) -> Value {
    let Some(endpoint) = state.session_recording_endpoint.as_ref() else {
        return Value::Bool(false);
    };

    json!({
        "endpoint": endpoint,
        "consoleLogRecordingEnabled": true,
        "proxy": true
    })
}

#[cfg_attr(target_arch = "wasm32", worker::send)]
async fn array_config(
    State(state): State<AppState>,
    Path(token): Path<String>,
) -> Result<Json<Value>, AppError> {
    Ok(Json(remote_config_response(&state, &token)))
}

#[cfg_attr(target_arch = "wasm32", worker::send)]
async fn array_config_js(
    State(state): State<AppState>,
    Path(token): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let config = remote_config_response(&state, &token);
    let token_json =
        serde_json::to_string(&token).map_err(|err| AppError::InvalidPayload(err.to_string()))?;
    let config_json =
        serde_json::to_string(&config).map_err(|err| AppError::InvalidPayload(err.to_string()))?;
    let body = format!(
        "(function() {{\n  window._POSTHOG_REMOTE_CONFIG = window._POSTHOG_REMOTE_CONFIG || {{}};\n  window._POSTHOG_REMOTE_CONFIG[{token_json}] = {{\n    config: {config_json},\n    siteApps: []\n  }}\n}})();\n"
    );

    Ok((
        [("content-type", "application/javascript; charset=utf-8")],
        body,
    )
        .into_response())
}

fn remote_config_response(state: &AppState, token: &str) -> Value {
    json!({
        "token": token,
        "supportedCompression": ["gzip", "gzip-js"],
        "hasFeatureFlags": !state.feature_flags.is_empty(),
        "analytics": { "endpoint": "/e/" },
        "captureDeadClicks": false,
        "capturePerformance": false,
        "autocapture_opt_out": false,
        "autocaptureExceptions": true,
        "elementsChainAsString": true,
        "errorTracking": {
            "autocaptureExceptions": true,
            "captureExtensionExceptions": false,
            "suppressionRules": []
        },
        "logs": { "captureConsoleLogs": false },
        "sessionRecording": session_recording_remote_config(state),
        "heatmaps": false,
        "conversations": false,
        "surveys": false,
        "productTours": false,
        "defaultIdentifiedOnly": true
    })
}

#[cfg_attr(target_arch = "wasm32", worker::send)]
async fn exception_autocapture_js() -> impl IntoResponse {
    (
        [("content-type", "application/javascript; charset=utf-8")],
        EXCEPTION_AUTOCAPTURE_JS,
    )
}

// Keep this asset aligned with tests/js_client/bun.lock. It is the upstream browser bundle
// served by posthog-js 1.373.4 and is redistributed under assets/vendor/POSTHOG-JS-LICENSE.
const EXCEPTION_AUTOCAPTURE_JS: &str =
    include_str!("../assets/vendor/posthog-exception-autocapture-1.373.4.js");

fn session_recording_remote_config(state: &AppState) -> Value {
    let Some(endpoint) = state.session_recording_endpoint.as_ref() else {
        return Value::Bool(false);
    };

    json!({
        "endpoint": endpoint,
        "consoleLogRecordingEnabled": true,
        "recorderVersion": "v2",
        "sampleRate": null,
        "minimumDurationMilliseconds": null,
        "networkPayloadCapture": null,
        "recordCanvas": false,
        "canvasFps": null,
        "canvasQuality": null,
        "scriptConfig": { "script": "posthog-recorder" },
        "version": 1,
        "urlTriggers": [],
        "urlBlocklist": [],
        "eventTriggers": [],
        "triggerMatchType": null,
        "masking": null,
        "linkedFlag": null
    })
}

#[cfg_attr(target_arch = "wasm32", worker::send)]
async fn session_recording(
    State(state): State<AppState>,
    enrichment: RequestEnrichment,
    axum::extract::Query(query): axum::extract::Query<SessionRecordingQuery>,
    payload: PostHogRawPayload,
) -> Result<impl IntoResponse, AppError> {
    let sent_at = payload.sent_at.clone();
    let items = expand_session_recording_values(payload.items)?;
    let mut events = Vec::new();
    let mut person_records = Vec::new();

    for item in items {
        let normalized = normalize_session_recording(item)?;
        let snapshot = ensure_person_snapshot(&state, &normalized.distinct_id).await?;
        let (person_id, person_created_at, person_properties) = person_fields(&snapshot);

        let event = PipelineEvent::from_session_recording(
            normalized.distinct_id,
            normalized.event,
            normalized.properties,
            normalized.api_key,
            normalized.timestamp,
            normalized.extra,
        )
        .with_team_id(state.posthog_team_id)
        .with_person(person_id, person_created_at, person_properties)
        .with_groups([None, None, None, None, None], None)
        .with_sent_at(sent_at.clone())
        .with_enrichment(enrichment.properties());
        push_person_record(&mut person_records, &snapshot, "session_recording", &event);
        events.push(event);
    }

    send_person_records(&state, person_records).await?;
    state.pipeline.send(events).await?;

    if query.beacon.as_deref() == Some("1") {
        Ok(StatusCode::NO_CONTENT.into_response())
    } else {
        Ok(Json(PostHogResponse::success()).into_response())
    }
}

#[derive(Default, Deserialize)]
struct SessionRecordingQuery {
    #[serde(default)]
    beacon: Option<String>,
}

struct NormalizedSessionRecording {
    event: String,
    distinct_id: String,
    api_key: Option<String>,
    timestamp: Option<chrono::DateTime<chrono::Utc>>,
    properties: Value,
    extra: HashMap<String, Value>,
}

fn expand_session_recording_values(values: Vec<Value>) -> Result<Vec<Value>, AppError> {
    let mut expanded = Vec::new();

    for value in values {
        let Value::Object(mut object) = value else {
            return Err(AppError::InvalidPayload(
                "expected session recording object".to_string(),
            ));
        };

        let Some(batch_value) = object.remove("batch") else {
            expanded.push(Value::Object(object));
            continue;
        };

        let batch = batch_value.as_array().ok_or_else(|| {
            AppError::InvalidPayload("expected session recording batch array".to_string())
        })?;
        let shared = object;

        for item in batch {
            let mut item_object = item.as_object().cloned().ok_or_else(|| {
                AppError::InvalidPayload("expected session recording batch item object".to_string())
            })?;

            for key in ["api_key", "token", "$token"] {
                if item_object.get(key).is_none() {
                    if let Some(value) = shared.get(key) {
                        item_object.insert(key.to_string(), value.clone());
                    }
                }
            }

            expanded.push(Value::Object(item_object));
        }
    }

    if expanded.is_empty() {
        return Err(AppError::InvalidPayload(
            "empty session recording batch".to_string(),
        ));
    }

    Ok(expanded)
}

fn normalize_session_recording(value: Value) -> Result<NormalizedSessionRecording, AppError> {
    let Value::Object(object) = value else {
        return Err(AppError::InvalidPayload(
            "expected session recording object".to_string(),
        ));
    };

    if object
        .get("properties")
        .and_then(Value::as_object)
        .is_none()
        && (object.get("metadata").is_some() || object.get("chunk").is_some())
    {
        return normalize_legacy_session_recording(object);
    }

    normalize_modern_session_recording(object)
}

fn normalize_legacy_session_recording(
    mut object: serde_json::Map<String, Value>,
) -> Result<NormalizedSessionRecording, AppError> {
    let api_key = recording_api_key(&object, None)
        .ok_or_else(|| AppError::InvalidPayload("missing session recording token".to_string()))?;
    let distinct_id = object
        .get("metadata")
        .and_then(Value::as_object)
        .and_then(|metadata| metadata.get("distinct_id"))
        .or_else(|| object.get("distinct_id"))
        .and_then(recording_id_string)
        .ok_or_else(|| AppError::InvalidPayload("missing distinct_id".to_string()))?;

    object.remove("api_key");
    object.remove("token");
    object.remove("$token");

    Ok(NormalizedSessionRecording {
        event: "$snapshot".to_string(),
        distinct_id,
        api_key: Some(api_key),
        timestamp: None,
        properties: json!({ "data": Value::Object(object) }),
        extra: HashMap::new(),
    })
}

fn normalize_modern_session_recording(
    object: serde_json::Map<String, Value>,
) -> Result<NormalizedSessionRecording, AppError> {
    let properties = object
        .get("properties")
        .and_then(Value::as_object)
        .ok_or_else(|| AppError::InvalidPayload("missing recording properties".to_string()))?;

    let api_key = recording_api_key(&object, Some(properties))
        .ok_or_else(|| AppError::InvalidPayload("missing session recording token".to_string()))?;
    let distinct_id = object
        .get("distinct_id")
        .or_else(|| object.get("$distinct_id"))
        .or_else(|| properties.get("distinct_id"))
        .or_else(|| properties.get("$distinct_id"))
        .and_then(recording_id_string)
        .ok_or_else(|| AppError::InvalidPayload("missing distinct_id".to_string()))?;

    let event = object
        .get("event")
        .and_then(Value::as_str)
        .unwrap_or("$snapshot");
    if event != "$snapshot" && event != "$snapshot_items" {
        return Err(AppError::InvalidPayload(format!(
            "unsupported session recording event {event}"
        )));
    }

    let session_id = properties
        .get("$session_id")
        .and_then(Value::as_str)
        .filter(|value| valid_session_id(value))
        .ok_or_else(|| AppError::InvalidPayload("missing or invalid $session_id".to_string()))?;
    let session_id_value = Value::String(session_id.to_string());
    let window_id_value = properties
        .get("$window_id")
        .cloned()
        .unwrap_or_else(|| session_id_value.clone());
    let snapshot_source = properties
        .get("$snapshot_source")
        .cloned()
        .unwrap_or_else(|| Value::String("web".to_string()));
    let snapshot_library = properties
        .get("$lib")
        .and_then(Value::as_str)
        .unwrap_or("web")
        .to_string();

    let snapshot_items = properties
        .get("$snapshot_items")
        .or_else(|| properties.get("$snapshot_data"))
        .ok_or_else(|| AppError::InvalidPayload("missing $snapshot_data".to_string()))
        .and_then(snapshot_items_from_value)?;

    let mut output = properties.clone();
    output.remove("$snapshot_data");
    output.insert(
        "distinct_id".to_string(),
        Value::String(distinct_id.clone()),
    );
    output.insert("$session_id".to_string(), session_id_value);
    output.insert("$window_id".to_string(), window_id_value);
    output.insert("$snapshot_source".to_string(), snapshot_source);
    output.insert("$snapshot_items".to_string(), Value::Array(snapshot_items));
    output.insert("$lib".to_string(), Value::String(snapshot_library));

    let mut extra = HashMap::new();
    if let Some(offset) = object.get("offset").cloned() {
        extra.insert("offset".to_string(), offset);
    }

    Ok(NormalizedSessionRecording {
        event: "$snapshot_items".to_string(),
        distinct_id,
        api_key: Some(api_key),
        timestamp: object.get("timestamp").and_then(recording_timestamp),
        properties: Value::Object(output),
        extra,
    })
}

fn recording_api_key(
    object: &serde_json::Map<String, Value>,
    properties: Option<&serde_json::Map<String, Value>>,
) -> Option<String> {
    ["api_key", "token", "$token"]
        .into_iter()
        .find_map(|key| object.get(key).and_then(Value::as_str))
        .or_else(|| {
            properties.and_then(|props| {
                ["api_key", "token", "$token"]
                    .into_iter()
                    .find_map(|key| props.get(key).and_then(Value::as_str))
            })
        })
        .map(str::to_string)
}

fn recording_id_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => {
            let value = value.trim();
            if value.is_empty() {
                None
            } else {
                Some(value.replace('\0', "\u{FFFD}"))
            }
        }
        Value::Number(_) | Value::Bool(_) => Some(value.to_string()),
        _ => None,
    }
}

fn valid_session_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 70
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-')
}

fn snapshot_items_from_value(value: &Value) -> Result<Vec<Value>, AppError> {
    match value {
        Value::Array(items) => Ok(items.clone()),
        Value::Object(item) => Ok(vec![Value::Object(item.clone())]),
        _ => Err(AppError::InvalidPayload(
            "missing $snapshot_data".to_string(),
        )),
    }
}

fn recording_timestamp(value: &Value) -> Option<chrono::DateTime<chrono::Utc>> {
    match value {
        Value::String(value) => chrono::DateTime::parse_from_rfc3339(value)
            .ok()
            .map(|timestamp| timestamp.with_timezone(&chrono::Utc)),
        Value::Number(value) => value
            .as_i64()
            .and_then(chrono::DateTime::<chrono::Utc>::from_timestamp_millis),
        _ => None,
    }
}

#[cfg_attr(target_arch = "wasm32", worker::send)]
async fn health() -> impl IntoResponse {
    Json(json!({ "status": "ok" }))
}

#[cfg_attr(target_arch = "wasm32", worker::send)]
async fn app_ui() -> impl IntoResponse {
    Html(ui::app_html())
}

#[cfg_attr(target_arch = "wasm32", worker::send)]
async fn replay_sessions(
    State(state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<ReplaySessionsQuery>,
) -> impl IntoResponse {
    let Some(client) = state.replay.as_ref() else {
        return replay_error_response(ReplayError::NotConfigured);
    };

    match client.list_sessions(query).await {
        Ok(response) => Json(response).into_response(),
        Err(err) => replay_error_response(err),
    }
}

#[cfg_attr(target_arch = "wasm32", worker::send)]
async fn product_analytics(
    State(state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<ProductAnalyticsQuery>,
) -> impl IntoResponse {
    let Some(client) = state.analytics.as_ref() else {
        return product_analytics_error_response(ProductAnalyticsError::NotConfigured);
    };

    match client.query(query).await {
        Ok(response) => Json(response).into_response(),
        Err(err) => product_analytics_error_response(err),
    }
}

#[derive(Debug, Deserialize)]
struct ErrorIssueStatusRequest {
    status: String,
    #[serde(default)]
    actor: Option<String>,
    #[serde(default)]
    reason: Option<String>,
    api_key: String,
}

#[cfg_attr(target_arch = "wasm32", worker::send)]
async fn update_error_issue_status(
    State(state): State<AppState>,
    Path(fingerprint): Path<String>,
    headers: HeaderMap,
    Json(payload): Json<ErrorIssueStatusRequest>,
) -> Result<Json<PostHogResponse>, AppError> {
    let Some(expected_token) = state.person_debug_token.as_deref() else {
        return Err(AppError::Unauthorized(
            "error tracking status updates are disabled".to_string(),
        ));
    };
    let provided_token = headers
        .get("x-hogflare-debug-token")
        .and_then(|value| value.to_str().ok())
        .map(str::trim);
    if provided_token != Some(expected_token) {
        return Err(AppError::Unauthorized(
            "invalid error tracking admin token".to_string(),
        ));
    }

    let fingerprint = fingerprint.trim();
    if fingerprint.is_empty() {
        return Err(AppError::InvalidPayload(
            "missing error issue fingerprint".to_string(),
        ));
    }

    let status = payload.status.trim().to_ascii_lowercase();
    if !matches!(status.as_str(), "active" | "resolved" | "ignored") {
        return Err(AppError::InvalidPayload(format!(
            "unsupported error issue status {status}"
        )));
    }

    let api_key = payload.api_key.trim();
    if api_key.is_empty() {
        return Err(AppError::InvalidPayload(
            "missing error issue project api_key".to_string(),
        ));
    }

    let mut properties = serde_json::Map::new();
    properties.insert(
        "fingerprint".to_string(),
        Value::String(fingerprint.to_string()),
    );
    properties.insert("status".to_string(), Value::String(status));
    if let Some(actor) = payload.actor.filter(|value| !value.trim().is_empty()) {
        properties.insert("actor".to_string(), Value::String(actor));
    }
    if let Some(reason) = payload.reason.filter(|value| !value.trim().is_empty()) {
        properties.insert("reason".to_string(), Value::String(reason));
    }

    let event = PipelineEvent::from_capture(CaptureRequest {
        api_key: Some(api_key.to_string()),
        event: "$error_issue_status".to_string(),
        distinct_id: format!("hogflare:error_issue:{fingerprint}"),
        properties: Some(Value::Object(properties)),
        timestamp: Some(Utc::now()),
        context: None,
        extra: HashMap::new(),
    })
    .as_hogflare_internal()
    .with_team_id(state.posthog_team_id);

    state.pipeline.send(vec![event]).await?;
    Ok(Json(PostHogResponse::success()))
}

#[cfg_attr(target_arch = "wasm32", worker::send)]
async fn replay_events(
    State(state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<ReplayEventsQuery>,
) -> impl IntoResponse {
    let Some(client) = state.replay.as_ref() else {
        return replay_error_response(ReplayError::NotConfigured);
    };

    match client.search_events(query).await {
        Ok(response) => Json(response).into_response(),
        Err(err) => replay_error_response(err),
    }
}

#[cfg_attr(target_arch = "wasm32", worker::send)]
async fn replay_funnels(
    State(state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<ReplayFunnelQuery>,
) -> impl IntoResponse {
    let Some(client) = state.replay.as_ref() else {
        return replay_error_response(ReplayError::NotConfigured);
    };

    match client.search_funnel(query).await {
        Ok(response) => Json(response).into_response(),
        Err(err) => replay_error_response(err),
    }
}

#[cfg_attr(target_arch = "wasm32", worker::send)]
async fn replay_friction(
    State(state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<ReplayFrictionQuery>,
) -> impl IntoResponse {
    let Some(client) = state.replay.as_ref() else {
        return replay_error_response(ReplayError::NotConfigured);
    };

    match client.search_friction(query).await {
        Ok(response) => Json(response).into_response(),
        Err(err) => replay_error_response(err),
    }
}

#[cfg_attr(target_arch = "wasm32", worker::send)]
async fn replay_person(
    State(state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<ReplayPersonQuery>,
) -> impl IntoResponse {
    let Some(client) = state.replay.as_ref() else {
        return replay_error_response(ReplayError::NotConfigured);
    };

    match client.person_journey(query).await {
        Ok(response) => Json(response).into_response(),
        Err(err) => replay_error_response(err),
    }
}

#[cfg_attr(target_arch = "wasm32", worker::send)]
async fn replay_session_events(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    axum::extract::Query(query): axum::extract::Query<ReplaySessionEventsQuery>,
) -> impl IntoResponse {
    let Some(client) = state.replay.as_ref() else {
        return replay_error_response(ReplayError::NotConfigured);
    };

    match client.get_session(&session_id, query).await {
        Ok(response) => Json(response).into_response(),
        Err(err) => replay_error_response(err),
    }
}

fn replay_error_response(err: ReplayError) -> axum::response::Response {
    let status = match &err {
        ReplayError::NotConfigured => StatusCode::SERVICE_UNAVAILABLE,
        ReplayError::SessionNotFound(_) => StatusCode::NOT_FOUND,
        ReplayError::UnexpectedResponse { .. }
        | ReplayError::Transport(_)
        | ReplayError::InvalidResponse(_)
        | ReplayError::MissingRows
        | ReplayError::InvalidRow(_) => StatusCode::BAD_GATEWAY,
        #[cfg(not(target_arch = "wasm32"))]
        ReplayError::ClientBuild(_) => StatusCode::INTERNAL_SERVER_ERROR,
        #[cfg(target_arch = "wasm32")]
        ReplayError::RequestBuild(_) | ReplayError::Serialize(_) => {
            StatusCode::INTERNAL_SERVER_ERROR
        }
    };

    if status.is_server_error() {
        error!(error = %err, "replay request failed");
    }

    (
        status,
        Json(ErrorResponse {
            status: 0,
            error: err.to_string(),
        }),
    )
        .into_response()
}

fn product_analytics_error_response(err: ProductAnalyticsError) -> axum::response::Response {
    let status = match &err {
        ProductAnalyticsError::NotConfigured => StatusCode::SERVICE_UNAVAILABLE,
        ProductAnalyticsError::Serialize(_) => StatusCode::INTERNAL_SERVER_ERROR,
        ProductAnalyticsError::Failed(_) | ProductAnalyticsError::InvalidResponse(_) => {
            StatusCode::BAD_GATEWAY
        }
        #[cfg(not(target_arch = "wasm32"))]
        ProductAnalyticsError::Process(_) => StatusCode::BAD_GATEWAY,
        #[cfg(target_arch = "wasm32")]
        ProductAnalyticsError::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
    };

    if status.is_server_error() {
        error!(error = %err, "analytics request failed");
    }

    (
        status,
        Json(ErrorResponse {
            status: 0,
            error: err.to_string(),
        }),
    )
        .into_response()
}

#[cfg_attr(target_arch = "wasm32", worker::send)]
async fn debug_person(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(distinct_id): Path<String>,
) -> impl IntoResponse {
    let Some(expected) = state.person_debug_token.as_deref() else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let provided = headers
        .get("x-hogflare-debug-token")
        .and_then(|value| value.to_str().ok())
        .map(str::trim);

    if provided != Some(expected) {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    match state.person_store.get_snapshot(&distinct_id).await {
        Ok(snapshot) => (StatusCode::OK, Json(snapshot)).into_response(),
        Err(err) => {
            error!(error = %err, "failed to load person record");
            let body = Json(ErrorResponse {
                status: 0,
                error: "failed to load person record".to_string(),
            });
            (StatusCode::INTERNAL_SERVER_ERROR, body).into_response()
        }
    }
}

#[derive(Debug, Error)]
pub enum RunError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Pipeline(#[from] PipelineError),
    #[error(transparent)]
    Replay(#[from] ReplayError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("server error: {0}")]
    Serve(String),
}

#[allow(dead_code)]
async fn apply_person_updates(
    state: &AppState,
    updates: Vec<PersonUpdate>,
) -> Result<Vec<persons::PersonSnapshot>, AppError> {
    let mut snapshots = Vec::new();
    for update in updates {
        if update.is_empty() {
            continue;
        }
        snapshots.push(state.person_store.apply_update(update).await?);
    }
    Ok(snapshots)
}

async fn apply_person_update(
    state: &AppState,
    update: PersonUpdate,
) -> Result<persons::PersonSnapshot, AppError> {
    if update.is_empty() {
        return state
            .person_store
            .ensure_person(&update.distinct_id)
            .await
            .map_err(Into::into);
    }
    Ok(state.person_store.apply_update(update).await?)
}

#[allow(dead_code)]
async fn apply_person_aliases(
    state: &AppState,
    aliases: Vec<PersonAlias>,
) -> Result<Vec<persons::PersonSnapshot>, AppError> {
    let mut snapshots = Vec::new();
    for alias in aliases {
        snapshots.push(state.person_store.apply_alias(alias).await?);
    }
    Ok(snapshots)
}

async fn ensure_person_snapshot(
    state: &AppState,
    distinct_id: &str,
) -> Result<persons::PersonSnapshot, AppError> {
    Ok(state.person_store.ensure_person(distinct_id).await?)
}

async fn evaluate_feature_flags(
    state: &AppState,
    payload: &DecideRequest,
) -> Result<feature_flags::FeatureFlagEvaluation, AppError> {
    if payload.disable_flags.unwrap_or(false) || state.feature_flags.is_empty() {
        return Ok(feature_flags::FeatureFlagEvaluation::empty());
    }

    let Some(distinct_id) = payload.distinct_id.clone() else {
        return Ok(feature_flags::FeatureFlagEvaluation::empty());
    };

    let snapshot = state.person_store.get_snapshot(&distinct_id).await?;
    let mut person_properties = serde_json::Map::new();
    if let Some(record) = snapshot.record {
        if let Value::Object(props) = record.merged_properties() {
            person_properties = props;
        }
    }

    if let Some(overrides) = payload.person_properties.as_ref() {
        for (key, value) in overrides {
            person_properties.insert(key.clone(), value.clone());
        }
    }
    person_properties
        .entry("distinct_id".to_string())
        .or_insert_with(|| Value::String(distinct_id.clone()));

    let groups = payload.groups.clone().unwrap_or_default();
    let mut group_properties: std::collections::HashMap<String, serde_json::Map<String, Value>> =
        std::collections::HashMap::new();

    for (group_type, group_key) in &groups {
        let snapshot = state
            .group_store
            .get_snapshot(group_type, group_key)
            .await?;
        if let Some(record) = snapshot.record {
            let mut props = record.properties;
            props
                .entry("$group_key".to_string())
                .or_insert_with(|| Value::String(group_key.clone()));
            group_properties.insert(group_type.clone(), props);
        }
    }

    if let Some(overrides) = payload.group_properties.as_ref() {
        for (group_type, props) in overrides {
            let converted: serde_json::Map<String, Value> =
                props.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            let merged = group_properties.entry(group_type.clone()).or_default();
            for (key, value) in converted {
                merged.insert(key, value);
            }
            if let Some(group_key) = groups.get(group_type) {
                merged
                    .entry("$group_key".to_string())
                    .or_insert_with(|| Value::String(group_key.clone()));
            }
        }
    }

    let ctx = FeatureFlagContext {
        distinct_id,
        person_properties,
        groups,
        group_properties,
    };

    let mut options = feature_flags::FeatureFlagEvaluationOptions::default();
    if let Some(keys) = payload.flag_keys_to_evaluate.as_ref() {
        options.flag_keys = Some(keys.iter().cloned().collect());
    }
    if let Some(envs) = payload.evaluation_environments.as_ref() {
        options.evaluation_environments = Some(envs.iter().cloned().collect());
    }
    if let Some(contexts) = payload.evaluation_contexts.as_ref() {
        options.evaluation_environments = Some(contexts.iter().cloned().collect());
    }

    Ok(state.feature_flags.evaluate_with(&ctx, &options))
}

fn person_fields(
    snapshot: &persons::PersonSnapshot,
) -> (
    Option<String>,
    Option<chrono::DateTime<chrono::Utc>>,
    Option<Value>,
) {
    match &snapshot.record {
        Some(record) => (
            Some(record.uuid.clone()),
            Some(record.created_at),
            Some(record.merged_properties()),
        ),
        None => (None, None, None),
    }
}

fn push_person_record(
    records: &mut Vec<PersonPipelineRecord>,
    snapshot: &persons::PersonSnapshot,
    operation: &str,
    event: &PipelineEvent,
) {
    if let Some(record) = PersonPipelineRecord::from_snapshot(snapshot, operation, event) {
        records.push(record);
    }
}

async fn send_person_records(
    state: &AppState,
    records: Vec<PersonPipelineRecord>,
) -> Result<(), AppError> {
    if records.is_empty() {
        return Ok(());
    }

    if let Some(pipeline) = state.persons_pipeline.as_ref() {
        pipeline.send_records(records).await?;
    }

    Ok(())
}

fn extract_groups(properties: &Option<Value>) -> Option<serde_json::Map<String, Value>> {
    let props = properties.as_ref()?.as_object()?;
    let groups = props.get("$groups")?.as_object()?;
    Some(groups.clone())
}

fn browser_distinct_id(payload: &BrowserCaptureRequest) -> Option<String> {
    payload.distinct_id.clone().or_else(|| {
        payload.properties.as_ref().and_then(|props| {
            props
                .get("$distinct_id")
                .or_else(|| props.get("distinct_id"))
                .and_then(Value::as_str)
                .map(String::from)
        })
    })
}

fn browser_identify_request(
    payload: BrowserCaptureRequest,
    api_key: Option<String>,
    distinct_id: String,
) -> IdentifyRequest {
    let mut extra = payload.extra;
    let properties_obj = payload.properties.as_ref().and_then(Value::as_object);

    let set = payload.set.or_else(|| {
        properties_obj
            .and_then(|props| props.get("$set"))
            .cloned()
            .or(payload.properties.clone())
    });

    if let Some(set_once) = payload.set_once.or_else(|| {
        properties_obj
            .and_then(|props| props.get("$set_once"))
            .cloned()
    }) {
        extra.insert("$set_once".to_string(), set_once);
    }

    let anon_distinct_id = properties_obj
        .and_then(|props| props.get("$anon_distinct_id"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            extra
                .get("$anon_distinct_id")
                .and_then(Value::as_str)
                .map(str::to_string)
        });

    IdentifyRequest {
        api_key,
        distinct_id,
        anon_distinct_id,
        properties: set,
        timestamp: payload.timestamp,
        context: None,
        extra,
    }
}

fn browser_group_identify_request(
    payload: BrowserCaptureRequest,
    api_key: Option<String>,
) -> Result<GroupIdentifyRequest, AppError> {
    group_identify_from_parts(
        api_key,
        payload.properties,
        payload.timestamp,
        payload.extra,
    )
}

fn group_identify_from_capture(payload: CaptureRequest) -> Result<GroupIdentifyRequest, AppError> {
    group_identify_from_parts(
        payload.api_key,
        payload.properties,
        payload.timestamp,
        payload.extra,
    )
}

fn group_identify_from_parts(
    api_key: Option<String>,
    properties: Option<Value>,
    timestamp: Option<chrono::DateTime<chrono::Utc>>,
    extra: std::collections::HashMap<String, Value>,
) -> Result<GroupIdentifyRequest, AppError> {
    let props = properties
        .as_ref()
        .and_then(Value::as_object)
        .ok_or_else(|| AppError::InvalidPayload("missing group identify properties".to_string()))?;

    let group_type = props
        .get("$group_type")
        .or_else(|| props.get("group_type"))
        .and_then(Value::as_str)
        .ok_or_else(|| AppError::InvalidPayload("missing $group_type".to_string()))?
        .to_string();
    let group_key = props
        .get("$group_key")
        .or_else(|| props.get("group_key"))
        .and_then(Value::as_str)
        .ok_or_else(|| AppError::InvalidPayload("missing $group_key".to_string()))?
        .to_string();
    let group_properties = props
        .get("$group_set")
        .or_else(|| props.get("properties"))
        .cloned()
        .or_else(|| Some(Value::Object(serde_json::Map::new())));

    Ok(GroupIdentifyRequest {
        api_key,
        group_type,
        group_key,
        properties: group_properties,
        timestamp,
        extra,
    })
}

fn normalize_group_identify_request(
    request: GroupIdentifyRequest,
) -> Result<GroupIdentifyRequest, AppError> {
    if !request.group_type.is_empty() && !request.group_key.is_empty() {
        return Ok(request);
    }

    group_identify_from_parts(
        request.api_key,
        request.properties,
        request.timestamp,
        request.extra,
    )
}

fn group_update_from_identify(request: &GroupIdentifyRequest) -> groups::GroupUpdate {
    let properties = request
        .properties
        .as_ref()
        .and_then(Value::as_object)
        .map(|props| {
            props
                .get("$group_set")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_else(|| props.clone())
        })
        .unwrap_or_default();

    groups::GroupUpdate {
        group_type: request.group_type.clone(),
        group_key: request.group_key.clone(),
        properties,
    }
}

fn group_fields_from_snapshot(
    group_type_map: &GroupTypeMap,
    snapshot: groups::GroupSnapshot,
) -> ([Option<String>; 5], Option<Value>) {
    let mut group_slots = [None, None, None, None, None];
    let mut group_properties = None;
    if let Some(record) = snapshot.record {
        if let Some(index) = group_type_map.index_for(&record.group_type) {
            group_slots[index] = Some(record.group_key.clone());
        }
        let mut props = serde_json::Map::new();
        props.insert(record.group_type.clone(), Value::Object(record.properties));
        group_properties = Some(Value::Object(props));
    }

    (group_slots, group_properties)
}

fn extract_group_set(value: Option<&Value>) -> serde_json::Map<String, Value> {
    let mut updates = serde_json::Map::new();
    if let Some(Value::Object(groups)) = value {
        for (group_type, props) in groups {
            if let Value::Object(props_map) = props {
                updates.insert(group_type.clone(), Value::Object(props_map.clone()));
            }
        }
    }
    updates
}

fn group_slots_from_map(
    group_type_map: &GroupTypeMap,
    groups: &serde_json::Map<String, Value>,
) -> [Option<String>; 5] {
    let mut slots: [Option<String>; 5] = [None, None, None, None, None];
    for (group_type, value) in groups {
        if let Some(group_key) = value.as_str() {
            if let Some(index) = group_type_map.index_for(group_type) {
                slots[index] = Some(group_key.to_string());
            }
        }
    }
    slots
}

async fn hydrate_group_properties(
    state: &AppState,
    groups: &serde_json::Map<String, Value>,
) -> Result<Option<Value>, AppError> {
    let mut props = serde_json::Map::new();
    for (group_type, value) in groups {
        let Some(group_key) = value.as_str() else {
            continue;
        };
        let snapshot = state
            .group_store
            .get_snapshot(group_type, group_key)
            .await?;
        if let Some(record) = snapshot.record {
            props.insert(group_type.clone(), Value::Object(record.properties));
        }
    }

    if props.is_empty() {
        Ok(None)
    } else {
        Ok(Some(Value::Object(props)))
    }
}

#[derive(Debug)]
enum BatchItemKind {
    Capture,
    Identify,
    Alias,
    Engage,
    GroupIdentify,
}

impl BatchItemKind {
    fn person_operation(&self) -> &'static str {
        match self {
            BatchItemKind::Capture => "capture",
            BatchItemKind::Identify => "identify",
            BatchItemKind::Alias => "alias",
            BatchItemKind::Engage => "engage",
            BatchItemKind::GroupIdentify => "group_identify",
        }
    }
}

struct BatchItem {
    #[allow(dead_code)]
    kind: BatchItemKind,
    event: PipelineEvent,
    person_update: Option<PersonUpdate>,
    alias: Option<PersonAlias>,
    anon_distinct_id: Option<String>,
    groups: Option<serde_json::Map<String, Value>>,
    group_set: serde_json::Map<String, Value>,
    group_update: Option<groups::GroupUpdate>,
}

fn convert_batch(
    batch: BatchRequest,
    shared_api_key: Option<String>,
) -> Result<Vec<BatchItem>, String> {
    let mut items = Vec::new();

    for value in batch.batch {
        items.push(convert_batch_item(value, shared_api_key.as_ref())?);
    }

    Ok(items)
}

fn normalize_capture_value(mut value: Value) -> Result<Value, String> {
    let map = value
        .as_object_mut()
        .ok_or_else(|| "expected JSON object in batch payload".to_string())?;
    if map.get("distinct_id").is_none() {
        if let Some(distinct_id) = map
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|props| {
                props
                    .get("distinct_id")
                    .or_else(|| props.get("$distinct_id"))
                    .and_then(Value::as_str)
            })
            .map(str::to_string)
        {
            map.insert("distinct_id".to_string(), Value::String(distinct_id));
        }
    }
    Ok(value)
}

fn normalize_alias_value(mut value: Value) -> Result<Value, String> {
    let map = value
        .as_object_mut()
        .ok_or_else(|| "expected JSON object in batch alias payload".to_string())?;
    if map.get("alias").is_none() {
        if let Some(alias) = map
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|props| props.get("alias").and_then(Value::as_str))
            .map(str::to_string)
        {
            map.insert("alias".to_string(), Value::String(alias));
        }
    }
    Ok(value)
}

fn normalize_group_identify_value(mut value: Value) -> Result<Value, String> {
    let map = value
        .as_object_mut()
        .ok_or_else(|| "expected JSON object in batch group identify payload".to_string())?;
    let props = map.get("properties").and_then(Value::as_object).cloned();

    if map.get("group_type").is_none() {
        if let Some(group_type) = props
            .as_ref()
            .and_then(|props| {
                props
                    .get("$group_type")
                    .or_else(|| props.get("group_type"))
                    .and_then(Value::as_str)
            })
            .map(str::to_string)
        {
            map.insert("group_type".to_string(), Value::String(group_type));
        }
    }

    if map.get("group_key").is_none() {
        if let Some(group_key) = props
            .as_ref()
            .and_then(|props| {
                props
                    .get("$group_key")
                    .or_else(|| props.get("group_key"))
                    .and_then(Value::as_str)
            })
            .map(str::to_string)
        {
            map.insert("group_key".to_string(), Value::String(group_key));
        }
    }

    if let Some(group_set) = props
        .as_ref()
        .and_then(|props| props.get("$group_set").cloned())
    {
        map.insert("properties".to_string(), group_set);
    } else if map.get("properties").is_none() {
        map.insert(
            "properties".to_string(),
            Value::Object(serde_json::Map::new()),
        );
    }

    Ok(value)
}

fn convert_batch_item(
    mut value: Value,
    shared_api_key: Option<&String>,
) -> Result<BatchItem, String> {
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
            .map(|request| {
                let update = update_from_identify(&request);
                let groups = extract_groups(&request.properties);
                let anon_distinct_id = anon_distinct_id_from_identify(&request);
                BatchItem {
                    kind: BatchItemKind::Identify,
                    event: PipelineEvent::from_identify(request),
                    person_update: update,
                    alias: None,
                    anon_distinct_id,
                    groups,
                    group_set: serde_json::Map::new(),
                    group_update: None,
                }
            })
            .map_err(|err| format!("invalid identify event: {err}"));
    }

    if matches!(type_field.as_deref(), Some("groupidentify"))
        || matches!(type_field.as_deref(), Some("group_identify"))
        || matches!(event_field.as_deref(), Some("$groupidentify"))
    {
        let value = normalize_group_identify_value(value)?;
        return serde_json::from_value::<GroupIdentifyRequest>(value)
            .map(|request| {
                let group_update = Some(group_update_from_identify(&request));
                BatchItem {
                    kind: BatchItemKind::GroupIdentify,
                    event: PipelineEvent::from_group_identify(request),
                    person_update: None,
                    alias: None,
                    anon_distinct_id: None,
                    groups: None,
                    group_set: serde_json::Map::new(),
                    group_update,
                }
            })
            .map_err(|err| format!("invalid group identify event: {err}"));
    }

    if matches!(type_field.as_deref(), Some("alias"))
        || matches!(event_field.as_deref(), Some("$create_alias"))
        || has_alias_fields
    {
        let value = normalize_alias_value(value)?;
        return serde_json::from_value::<AliasRequest>(value)
            .map(|request| {
                let alias = alias_from_request(&request);
                BatchItem {
                    kind: BatchItemKind::Alias,
                    event: PipelineEvent::from_alias(request),
                    person_update: None,
                    alias: Some(alias),
                    anon_distinct_id: None,
                    groups: None,
                    group_set: serde_json::Map::new(),
                    group_update: None,
                }
            })
            .map_err(|err| format!("invalid alias event: {err}"));
    }

    if matches!(type_field.as_deref(), Some("engage")) {
        return serde_json::from_value::<EngageRequest>(value)
            .map(|request| {
                let update = update_from_engage(&request);
                let groups = request
                    .extra
                    .get("$groups")
                    .and_then(|value| value.as_object())
                    .cloned();
                let group_set = extract_group_set(request.group_set.as_ref());
                BatchItem {
                    kind: BatchItemKind::Engage,
                    event: PipelineEvent::from_engage(request),
                    person_update: update,
                    alias: None,
                    anon_distinct_id: None,
                    groups,
                    group_set,
                    group_update: None,
                }
            })
            .map_err(|err| format!("invalid engage event: {err}"));
    }

    let value = normalize_capture_value(value)?;
    serde_json::from_value::<CaptureRequest>(value)
        .map(|request| {
            let update = update_from_capture(&request);
            let groups = extract_groups(&request.properties);
            let group_set = if let Some(Value::Object(props)) = request.properties.as_ref() {
                extract_group_set(props.get("$group_set"))
            } else {
                serde_json::Map::new()
            };
            BatchItem {
                kind: BatchItemKind::Capture,
                event: PipelineEvent::from_capture(request),
                person_update: update,
                alias: None,
                anon_distinct_id: None,
                groups,
                group_set,
                group_update: None,
            }
        })
        .map_err(|err| format!("invalid capture event: {err}"))
}

fn api_key_from_browser_properties(properties: Option<&Value>) -> Option<String> {
    properties.and_then(|props| {
        props
            .get("token")
            .or_else(|| props.get("api_key"))
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(String::from)
    })
}

fn anon_distinct_id_from_identify(request: &IdentifyRequest) -> Option<String> {
    anon_distinct_id_from_parts(
        request.anon_distinct_id.as_deref(),
        request.properties.as_ref(),
        &request.extra,
    )
}

fn anon_distinct_id_from_parts(
    explicit: Option<&str>,
    properties: Option<&Value>,
    extra: &std::collections::HashMap<String, Value>,
) -> Option<String> {
    explicit
        .map(str::to_string)
        .or_else(|| {
            properties
                .and_then(Value::as_object)
                .and_then(|props| props.get("$anon_distinct_id"))
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .or_else(|| {
            extra
                .get("$anon_distinct_id")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
}
