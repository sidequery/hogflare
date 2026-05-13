pub mod config;
pub mod extractors;
pub mod feature_flags;
pub mod groups;
pub mod models;
pub mod persons;
pub mod pipeline;

use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use chrono::Utc;
use config::{Config, ConfigError};
use extractors::{
    header_api_key, header_sent_at, verify_signature, ApplyApiKey, PostHogBatchPayload,
    PostHogPayload, RequestEnrichment,
};
use feature_flags::{FeatureFlagContext, FeatureFlagStore};
use groups::{GroupError, GroupStore, GroupTypeMap, NoopGroupStore};
use models::{
    AliasRequest, BatchRequest, CaptureRequest, DecideResponse, DecideSessionRecording,
    EngageRequest, ErrorResponse, GroupIdentifyRequest, IdentifyRequest, PostHogResponse,
};
use persons::{
    alias_from_request, update_from_capture, update_from_engage, update_from_identify,
    NoopPersonStore, PersonAlias, PersonError, PersonStore, PersonUpdate,
};
use pipeline::{PipelineClient, PipelineError, PipelineEvent};
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
        config.posthog_team_id,
        Arc::new(NoopGroupStore),
        GroupTypeMap::new(config.posthog_group_types.clone()),
        config.posthog_project_api_key.clone(),
        config.session_recording_endpoint.clone(),
        config.posthog_signing_secret.clone(),
        config.person_debug_token.clone(),
        Arc::new(config.feature_flags),
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

    let person_store: Arc<dyn PersonStore> = persons::store_from_env(&env, config.posthog_team_id);
    let group_store: Arc<dyn GroupStore> = groups::store_from_env(&env);
    let group_type_map = GroupTypeMap::new(config.posthog_group_types.clone());
    let feature_flags = Arc::new(config.feature_flags);

    let mut router = build_router_with_options(
        Arc::new(pipeline),
        config.posthog_team_id,
        group_store,
        group_type_map,
        config.posthog_project_api_key.clone(),
        config.session_recording_endpoint.clone(),
        config.posthog_signing_secret.clone(),
        config.person_debug_token.clone(),
        feature_flags,
        person_store,
    );

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
    router(build_state(
        pipeline,
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
    let state = build_state(
        pipeline,
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
        .route("/capture", post(capture))
        .route("/e", post(browser_capture))
        .route("/e/", post(browser_capture))
        .route("/identify", post(identify))
        .route("/batch", post(batch))
        .route("/batch/", post(batch))
        .route("/groups", post(groups))
        .route("/alias", post(alias))
        .route("/engage", post(engage))
        .route("/decide", post(decide))
        .route("/flags", post(flags))
        .route("/flags/", post(flags))
        .route("/s", post(session_recording))
        .route("/s/", post(session_recording))
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
        events.push(
            PipelineEvent::from_capture(item)
                .with_team_id(state.posthog_team_id)
                .with_person(person_id, person_created_at, person_properties)
                .with_groups(group_slots, group_properties)
                .with_sent_at(sent_at.clone())
                .with_enrichment(enrichment),
        );
    }

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

    for payload in payload.items {
        let api_key = payload.token.clone().or(payload.api_key.clone());

        let distinct_id = browser_distinct_id(&payload)
            .ok_or_else(|| AppError::InvalidPayload("missing distinct_id".into()))?;

        let event = if payload.event == "$identify" {
            let identify_req = browser_identify_request(payload, api_key, distinct_id);
            if let Some(anon) = identify_req
                .anon_distinct_id
                .clone()
                .or_else(|| {
                    identify_req
                        .properties
                        .as_ref()
                        .and_then(Value::as_object)
                        .and_then(|props| props.get("$anon_distinct_id"))
                        .and_then(Value::as_str)
                        .map(str::to_string)
                })
                .or_else(|| {
                    identify_req
                        .extra
                        .get("$anon_distinct_id")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                })
            {
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

        events.push(
            event
                .with_team_id(state.posthog_team_id)
                .with_person(person_id, person_created_at, person_properties)
                .with_groups(group_slots, group_properties)
                .with_sent_at(sent_at.clone())
                .with_enrichment(enrichment),
        );
    }

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

    for item in payload.items {
        if let Some(anon) = item
            .anon_distinct_id
            .clone()
            .or_else(|| {
                item.properties
                    .as_ref()
                    .and_then(Value::as_object)
                    .and_then(|props| props.get("$anon_distinct_id"))
                    .and_then(Value::as_str)
                    .map(|value| value.to_string())
            })
            .or_else(|| {
                item.extra
                    .get("$anon_distinct_id")
                    .and_then(Value::as_str)
                    .map(|value| value.to_string())
            })
        {
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
        events.push(
            PipelineEvent::from_identify(item)
                .with_team_id(state.posthog_team_id)
                .with_person(person_id, person_created_at, person_properties)
                .with_groups(group_slots, group_properties)
                .with_sent_at(sent_at.clone())
                .with_enrichment(enrichment),
        );
    }

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

    for item in items {
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
        events.push(event);
    }

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

    for item in payload.items {
        let snapshot = state
            .person_store
            .apply_alias(alias_from_request(&item))
            .await?;
        let (person_id, person_created_at, person_properties) = person_fields(&snapshot);
        events.push(
            PipelineEvent::from_alias(item)
                .with_team_id(state.posthog_team_id)
                .with_person(person_id, person_created_at, person_properties)
                .with_groups([None, None, None, None, None], None)
                .with_sent_at(sent_at.clone())
                .with_enrichment(enrichment),
        );
    }

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
        events.push(
            PipelineEvent::from_engage(item)
                .with_team_id(state.posthog_team_id)
                .with_person(person_id, person_created_at, person_properties)
                .with_groups(group_slots, group_properties)
                .with_sent_at(sent_at.clone())
                .with_enrichment(enrichment),
        );
    }

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

    if let Some(endpoint) = state.session_recording_endpoint.clone() {
        response.session_recording.endpoint = Some(endpoint);
        response.session_recording.proxy = true;
    }

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
    session_recording: Option<DecideSessionRecording>,
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
        let mut recording = DecideSessionRecording::default();
        recording.proxy = true;
        if let Some(endpoint) = state.session_recording_endpoint.clone() {
            recording.endpoint = Some(endpoint);
        }
        session_recording = Some(recording);

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

#[cfg_attr(target_arch = "wasm32", worker::send)]
async fn session_recording(
    State(state): State<AppState>,
    enrichment: RequestEnrichment,
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

    let snapshot = ensure_person_snapshot(&state, &distinct_id).await?;
    let (person_id, person_created_at, person_properties) = person_fields(&snapshot);

    let event = PipelineEvent::from_session_recording(distinct_id, payload, api_key)
        .with_team_id(state.posthog_team_id)
        .with_person(person_id, person_created_at, person_properties)
        .with_groups([None, None, None, None, None], None)
        .with_sent_at(sent_at)
        .with_enrichment(enrichment.properties());

    state.pipeline.send(vec![event]).await?;
    Ok(Json(PostHogResponse::success()))
}

#[cfg_attr(target_arch = "wasm32", worker::send)]
async fn health() -> impl IntoResponse {
    Json(json!({ "status": "ok" }))
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
                let anon_distinct_id = request.anon_distinct_id.clone();
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
