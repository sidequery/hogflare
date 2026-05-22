use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::time::Duration;

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine};
use chrono::{DateTime, Utc};
use flate2::read::GzDecoder;
use http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use thiserror::Error;
use url::Url;

#[cfg(not(target_arch = "wasm32"))]
use reqwest::Client;

#[cfg(target_arch = "wasm32")]
use worker::{
    wasm_bindgen::JsValue, wasm_bindgen_futures::spawn_local, AbortController, Delay, Fetch,
    Headers, Method, Request, RequestInit,
};

const DEFAULT_REPLAY_TABLE: &str = "default.hogflare_events";
const DEFAULT_REPLAY_QUERY_LIMIT: usize = 5_000;
const REPLAY_SQL_TIMEOUT: Duration = Duration::from_secs(20);
const SNAPSHOT_EVENT: &str = "$snapshot";
const SNAPSHOT_ITEMS_EVENT: &str = "$snapshot_items";

#[derive(Debug, Clone)]
pub struct ReplayConfig {
    pub account_id: String,
    pub bucket_name: String,
    pub auth_token: String,
    pub events_table: String,
    pub query_limit: usize,
    pub endpoint: Url,
}

impl ReplayConfig {
    pub fn new(
        account_id: String,
        bucket_name: String,
        auth_token: String,
        events_table: Option<String>,
        query_limit: Option<usize>,
        endpoint: Option<Url>,
    ) -> Result<Self, ReplayConfigError> {
        let events_table = events_table.unwrap_or_else(|| DEFAULT_REPLAY_TABLE.to_string());
        validate_sql_identifier(&events_table)?;

        let endpoint = match endpoint {
            Some(endpoint) => endpoint,
            None => Url::parse(&format!(
                "https://api.sql.cloudflarestorage.com/api/v1/accounts/{}/r2-sql/query/{}",
                account_id, bucket_name
            ))
            .map_err(|err| ReplayConfigError::InvalidEndpoint {
                value: "<generated-r2-sql-endpoint>".to_string(),
                message: err.to_string(),
            })?,
        };

        Ok(Self {
            account_id,
            bucket_name,
            auth_token,
            events_table,
            query_limit: query_limit.unwrap_or(DEFAULT_REPLAY_QUERY_LIMIT),
            endpoint,
        })
    }
}

#[derive(Debug, Error)]
pub enum ReplayConfigError {
    #[error("invalid replay events table `{value}`")]
    InvalidEventsTable { value: String },
    #[error("invalid replay endpoint `{value}`: {message}")]
    InvalidEndpoint { value: String, message: String },
    #[error("invalid replay query limit `{value}`: {message}")]
    InvalidQueryLimit { value: String, message: String },
}

#[derive(Debug, Clone)]
pub struct ReplayClient {
    config: ReplayConfig,
    #[cfg(not(target_arch = "wasm32"))]
    client: Client,
}

impl ReplayClient {
    pub fn new(config: ReplayConfig) -> Result<Self, ReplayError> {
        #[cfg(not(target_arch = "wasm32"))]
        let client = Client::builder()
            .timeout(REPLAY_SQL_TIMEOUT)
            .build()
            .map_err(ReplayError::ClientBuild)?;

        Ok(Self {
            config,
            #[cfg(not(target_arch = "wasm32"))]
            client,
        })
    }

    pub async fn list_sessions(
        &self,
        query: ReplaySessionsQuery,
    ) -> Result<ReplaySessionsResponse, ReplayError> {
        let mut rows = self.query_snapshot_rows(&query).await?;
        if let Some(session_id) = query.session_id.as_ref().filter(|value| !value.is_empty()) {
            rows.retain(|row| session_id_for_row(row) == *session_id);
        }
        let mut response = build_sessions_response(rows, self.config.query_limit);
        apply_session_summary_filters(&mut response.sessions, &query);

        if let Some(event_name) = query.event_name.as_ref().filter(|value| !value.is_empty()) {
            let event_query = ReplayEventsQuery {
                api_key: query.api_key.clone(),
                distinct_id: query.distinct_id.clone(),
                session_id: query.session_id.clone(),
                event_name: Some(event_name.clone()),
                url: query.url.clone(),
                date_from: query.date_from.clone(),
                date_to: query.date_to.clone(),
                limit: Some(self.config.query_limit),
            };
            let events = self.query_event_rows(&event_query).await?;
            let matching_sessions = matching_session_keys_from_events(events);
            response.sessions.retain(|session| {
                matching_sessions.session_ids.contains(&session.session_id)
                    || matching_sessions
                        .distinct_ids
                        .contains(&session.distinct_id)
            });
        }

        response.sessions.truncate(query.limit_or_default());
        Ok(response)
    }

    pub async fn get_session(
        &self,
        session_id: &str,
        query: ReplaySessionEventsQuery,
    ) -> Result<ReplaySessionEventsResponse, ReplayError> {
        let rows = self
            .query_snapshot_rows(&ReplaySessionsQuery {
                api_key: query.api_key,
                distinct_id: query.distinct_id,
                session_id: Some(session_id.to_string()),
                url: None,
                event_name: None,
                date_from: None,
                date_to: None,
                min_duration_secs: None,
                max_duration_secs: None,
                min_events: None,
                max_events: None,
                limit: query.limit,
            })
            .await?;

        build_session_events_response(session_id, rows, query.at_ms)
    }

    pub async fn search_events(
        &self,
        query: ReplayEventsQuery,
    ) -> Result<ReplayEventsResponse, ReplayError> {
        let rows = self.query_event_rows(&query).await?;
        Ok(build_events_response(rows, query.limit_or_default()))
    }

    pub async fn search_funnel(
        &self,
        query: ReplayFunnelQuery,
    ) -> Result<ReplayFunnelResponse, ReplayError> {
        let rows = self.query_event_rows(&query.to_events_query()).await?;
        Ok(build_funnel_response(
            rows,
            query.steps_vec(),
            query.limit_or_default(),
        ))
    }

    pub async fn search_friction(
        &self,
        query: ReplayFrictionQuery,
    ) -> Result<ReplayFrictionResponse, ReplayError> {
        let sessions_query = query.to_sessions_query();
        let mut rows = self.query_snapshot_rows(&sessions_query).await?;
        retain_snapshot_rows_for_session(&mut rows, sessions_query.session_id.as_deref());

        let mut session_response = build_sessions_response(rows.clone(), self.config.query_limit);
        apply_session_summary_filters(&mut session_response.sessions, &sessions_query);
        let allowed_sessions = session_response
            .sessions
            .into_iter()
            .map(|session| session.session_id)
            .collect::<HashSet<_>>();
        rows.retain(|row| allowed_sessions.contains(&session_id_for_row(row)));

        Ok(build_friction_response(
            rows,
            query.signal.as_deref(),
            query.limit_or_default(),
        ))
    }

    pub async fn person_journey(
        &self,
        query: ReplayPersonQuery,
    ) -> Result<ReplayPersonJourneyResponse, ReplayError> {
        let sessions_query = query.to_sessions_query();
        let events_query = query.to_events_query();
        let mut snapshot_rows = self.query_snapshot_rows(&sessions_query).await?;
        retain_snapshot_rows_for_session(&mut snapshot_rows, sessions_query.session_id.as_deref());

        let mut sessions = build_sessions_response(snapshot_rows, self.config.query_limit).sessions;
        apply_session_summary_filters(&mut sessions, &sessions_query);
        sessions.truncate(query.limit_or_default());

        let events = build_events_response(
            self.query_event_rows(&events_query).await?,
            query.limit_or_default(),
        )
        .events;

        Ok(build_person_journey_response(
            query.distinct_id.clone().filter(|value| !value.is_empty()),
            sessions,
            events,
            query.limit_or_default(),
        ))
    }

    async fn query_snapshot_rows(
        &self,
        query: &ReplaySessionsQuery,
    ) -> Result<Vec<ReplaySnapshotRow>, ReplayError> {
        let limit = query.limit_or_default().min(self.config.query_limit);
        let sql = build_snapshot_rows_sql(&self.config.events_table, query, limit);
        let response = self.query_sql(&sql).await?;
        let rows = rows_from_r2_sql_response(response)?;
        let mut rows = rows
            .into_iter()
            .map(ReplaySnapshotRow::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        rows.retain(|row| {
            row.event
                .as_deref()
                .map(is_replay_snapshot_event)
                .unwrap_or(true)
        });
        if let Some(api_key) = query.api_key.as_ref().filter(|value| !value.is_empty()) {
            rows.retain(|row| row.api_key.as_deref() == Some(api_key.as_str()));
        }
        if let Some(distinct_id) = query.distinct_id.as_ref().filter(|value| !value.is_empty()) {
            rows.retain(|row| row.distinct_id == *distinct_id);
        }

        Ok(rows)
    }

    async fn query_event_rows(
        &self,
        query: &ReplayEventsQuery,
    ) -> Result<Vec<ReplayEventRow>, ReplayError> {
        let limit = query.limit_or_default().min(self.config.query_limit);
        let sql = build_event_rows_sql(&self.config.events_table, query, limit);
        let response = self.query_sql(&sql).await?;
        let rows = rows_from_r2_sql_response(response)?;
        let mut rows = rows
            .into_iter()
            .map(ReplayEventRow::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        rows.retain(|row| !is_replay_snapshot_event(&row.event));
        if let Some(event_name) = query.event_name.as_ref().filter(|value| !value.is_empty()) {
            rows.retain(|row| row.event == *event_name);
        }
        if let Some(api_key) = query.api_key.as_ref().filter(|value| !value.is_empty()) {
            rows.retain(|row| row.api_key.as_deref() == Some(api_key.as_str()));
        }
        if let Some(distinct_id) = query.distinct_id.as_ref().filter(|value| !value.is_empty()) {
            rows.retain(|row| row.distinct_id == *distinct_id);
        }
        if let Some(session_id) = query.session_id.as_ref().filter(|value| !value.is_empty()) {
            rows.retain(|row| {
                session_id_from_value(&row.properties).as_deref() == Some(session_id)
            });
        }
        if let Some(url) = query.url.as_ref().filter(|value| !value.is_empty()) {
            rows.retain(|row| {
                event_url_from_properties(&row.properties)
                    .map(|candidate| contains_case_insensitive(&candidate, url))
                    .unwrap_or(false)
            });
        }

        Ok(rows)
    }

    async fn query_sql(&self, sql: &str) -> Result<Value, ReplayError> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let response = self
                .client
                .post(self.config.endpoint.clone())
                .bearer_auth(&self.config.auth_token)
                .json(&json!({ "query": sql }))
                .send()
                .await
                .map_err(ReplayError::Transport)?;

            let status = StatusCode::from_u16(response.status().as_u16())
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            let body = response.text().await.unwrap_or_default();

            if !status.is_success() {
                return Err(ReplayError::UnexpectedResponse { status, body });
            }

            serde_json::from_str(&body).map_err(ReplayError::InvalidResponse)
        }

        #[cfg(target_arch = "wasm32")]
        {
            let headers = Headers::new();
            headers
                .set("content-type", "application/json")
                .map_err(ReplayError::RequestBuild)?;
            headers
                .set(
                    "authorization",
                    &format!("Bearer {}", self.config.auth_token),
                )
                .map_err(ReplayError::RequestBuild)?;

            let body =
                serde_json::to_string(&json!({ "query": sql })).map_err(ReplayError::Serialize)?;
            let mut init = RequestInit::new();
            init.with_method(Method::Post);
            init.with_headers(headers);
            init.with_body(Some(JsValue::from_str(&body)));

            let request = Request::new_with_init(self.config.endpoint.as_str(), &init)
                .map_err(ReplayError::RequestBuild)?;
            let controller = AbortController::default();
            let signal = controller.signal();
            spawn_local(async move {
                Delay::from(REPLAY_SQL_TIMEOUT).await;
                controller.abort();
            });

            let mut response = Fetch::Request(request)
                .send_with_signal(&signal)
                .await
                .map_err(ReplayError::Transport)?;

            let status = StatusCode::from_u16(response.status_code())
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            let body = response.text().await.unwrap_or_default();

            if !status.is_success() {
                return Err(ReplayError::UnexpectedResponse { status, body });
            }

            serde_json::from_str(&body).map_err(ReplayError::InvalidResponse)
        }
    }
}

#[derive(Debug, Error)]
pub enum ReplayError {
    #[error("session replay is not configured")]
    NotConfigured,
    #[error("failed to create replay HTTP client: {0}")]
    #[cfg(not(target_arch = "wasm32"))]
    ClientBuild(#[source] reqwest::Error),
    #[error("failed to query replay rows: {0}")]
    #[cfg(not(target_arch = "wasm32"))]
    Transport(#[source] reqwest::Error),
    #[error("failed to query replay rows: {0}")]
    #[cfg(target_arch = "wasm32")]
    Transport(#[source] worker::Error),
    #[error("failed to build replay request: {0}")]
    #[cfg(target_arch = "wasm32")]
    RequestBuild(#[source] worker::Error),
    #[error("failed to serialize replay request: {0}")]
    #[cfg(target_arch = "wasm32")]
    Serialize(#[source] serde_json::Error),
    #[error("replay query responded with {status}: {body}")]
    UnexpectedResponse { status: StatusCode, body: String },
    #[error("invalid replay query response: {0}")]
    InvalidResponse(#[source] serde_json::Error),
    #[error("replay query response did not include rows")]
    MissingRows,
    #[error("invalid replay row: {0}")]
    InvalidRow(String),
    #[error("session `{0}` was not found")]
    SessionNotFound(String),
}

#[derive(Debug, Deserialize)]
pub struct ReplaySessionsQuery {
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub distinct_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub event_name: Option<String>,
    #[serde(default)]
    pub date_from: Option<String>,
    #[serde(default)]
    pub date_to: Option<String>,
    #[serde(default)]
    pub min_duration_secs: Option<i64>,
    #[serde(default)]
    pub max_duration_secs: Option<i64>,
    #[serde(default)]
    pub min_events: Option<usize>,
    #[serde(default)]
    pub max_events: Option<usize>,
    #[serde(default)]
    pub limit: Option<usize>,
}

impl ReplaySessionsQuery {
    fn limit_or_default(&self) -> usize {
        self.limit
            .unwrap_or(250)
            .clamp(1, DEFAULT_REPLAY_QUERY_LIMIT)
    }
}

#[derive(Debug, Deserialize)]
pub struct ReplaySessionEventsQuery {
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub distinct_id: Option<String>,
    #[serde(default)]
    pub at_ms: Option<i64>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct ReplayEventsQuery {
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub distinct_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub event_name: Option<String>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub date_from: Option<String>,
    #[serde(default)]
    pub date_to: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

impl ReplayEventsQuery {
    fn limit_or_default(&self) -> usize {
        self.limit
            .unwrap_or(100)
            .clamp(1, DEFAULT_REPLAY_QUERY_LIMIT)
    }
}

#[derive(Debug, Deserialize)]
pub struct ReplayFunnelQuery {
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub distinct_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub steps: Option<String>,
    #[serde(default)]
    pub date_from: Option<String>,
    #[serde(default)]
    pub date_to: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

impl ReplayFunnelQuery {
    fn limit_or_default(&self) -> usize {
        self.limit
            .unwrap_or(100)
            .clamp(1, DEFAULT_REPLAY_QUERY_LIMIT)
    }

    fn steps_vec(&self) -> Vec<String> {
        split_steps(self.steps.as_deref())
    }

    fn to_events_query(&self) -> ReplayEventsQuery {
        ReplayEventsQuery {
            api_key: self.api_key.clone(),
            distinct_id: self.distinct_id.clone(),
            session_id: self.session_id.clone(),
            event_name: None,
            url: self.url.clone(),
            date_from: self.date_from.clone(),
            date_to: self.date_to.clone(),
            limit: Some(self.limit_or_default()),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ReplayFrictionQuery {
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub distinct_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub signal: Option<String>,
    #[serde(default)]
    pub date_from: Option<String>,
    #[serde(default)]
    pub date_to: Option<String>,
    #[serde(default)]
    pub min_duration_secs: Option<i64>,
    #[serde(default)]
    pub max_duration_secs: Option<i64>,
    #[serde(default)]
    pub min_events: Option<usize>,
    #[serde(default)]
    pub max_events: Option<usize>,
    #[serde(default)]
    pub limit: Option<usize>,
}

impl ReplayFrictionQuery {
    fn limit_or_default(&self) -> usize {
        self.limit
            .unwrap_or(100)
            .clamp(1, DEFAULT_REPLAY_QUERY_LIMIT)
    }

    fn to_sessions_query(&self) -> ReplaySessionsQuery {
        ReplaySessionsQuery {
            api_key: self.api_key.clone(),
            distinct_id: self.distinct_id.clone(),
            session_id: self.session_id.clone(),
            url: self.url.clone(),
            event_name: None,
            date_from: self.date_from.clone(),
            date_to: self.date_to.clone(),
            min_duration_secs: self.min_duration_secs,
            max_duration_secs: self.max_duration_secs,
            min_events: self.min_events,
            max_events: self.max_events,
            limit: Some(self.limit_or_default()),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ReplayPersonQuery {
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub distinct_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub event_name: Option<String>,
    #[serde(default)]
    pub date_from: Option<String>,
    #[serde(default)]
    pub date_to: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

impl ReplayPersonQuery {
    fn limit_or_default(&self) -> usize {
        self.limit
            .unwrap_or(100)
            .clamp(1, DEFAULT_REPLAY_QUERY_LIMIT)
    }

    fn to_sessions_query(&self) -> ReplaySessionsQuery {
        ReplaySessionsQuery {
            api_key: self.api_key.clone(),
            distinct_id: self.distinct_id.clone(),
            session_id: self.session_id.clone(),
            url: self.url.clone(),
            event_name: self.event_name.clone(),
            date_from: self.date_from.clone(),
            date_to: self.date_to.clone(),
            min_duration_secs: None,
            max_duration_secs: None,
            min_events: None,
            max_events: None,
            limit: Some(self.limit_or_default()),
        }
    }

    fn to_events_query(&self) -> ReplayEventsQuery {
        ReplayEventsQuery {
            api_key: self.api_key.clone(),
            distinct_id: self.distinct_id.clone(),
            session_id: self.session_id.clone(),
            event_name: self.event_name.clone(),
            url: self.url.clone(),
            date_from: self.date_from.clone(),
            date_to: self.date_to.clone(),
            limit: Some(self.limit_or_default()),
        }
    }
}

#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct ReplaySessionSummary {
    pub session_id: String,
    pub distinct_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub duration_ms: i64,
    pub chunk_count: usize,
    pub event_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_url: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ReplaySessionsResponse {
    pub sessions: Vec<ReplaySessionSummary>,
}

#[derive(Debug, Serialize)]
pub struct ReplaySessionEventsResponse {
    pub session: ReplaySessionSummary,
    pub events: Vec<Value>,
    pub activity: Vec<ReplayActivityItem>,
    pub chunks: Vec<ReplayChunkSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replay_start_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replay_end_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replay_anchor_ms: Option<i64>,
}

#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct ReplayChunkSummary {
    pub uuid: String,
    pub created_at: DateTime<Utc>,
    pub event_count: usize,
    pub source_shape: String,
}

#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct ReplayActivityItem {
    pub id: String,
    pub timestamp: i64,
    pub offset_ms: i64,
    pub kind: String,
    pub label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    pub replay_anchor_ms: i64,
}

#[derive(Debug, Serialize)]
pub struct ReplayEventsResponse {
    pub events: Vec<ReplayAnalyticsEvent>,
}

#[derive(Debug, Serialize)]
pub struct ReplayFunnelResponse {
    pub steps: Vec<String>,
    pub sessions: Vec<ReplayFunnelSession>,
}

#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct ReplayFunnelSession {
    pub distinct_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub status: String,
    pub completed_steps: usize,
    pub step_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_step: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub drop_off_step: Option<String>,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    pub replay_anchor_ms: i64,
    pub steps: Vec<ReplayFunnelStepEvent>,
}

#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct ReplayFunnelStepEvent {
    pub step_index: usize,
    pub event: String,
    pub created_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ReplayFrictionResponse {
    pub sessions: Vec<ReplayFrictionSession>,
}

#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct ReplayFrictionSession {
    pub session: ReplaySessionSummary,
    pub score: usize,
    pub signals: Vec<ReplayFrictionSignal>,
}

#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct ReplayFrictionSignal {
    pub kind: String,
    pub label: String,
    pub detail: String,
    pub severity: String,
    pub count: usize,
    pub timestamp: i64,
    pub replay_anchor_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ReplayPersonJourneyResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub distinct_id: Option<String>,
    pub sessions: Vec<ReplaySessionSummary>,
    pub events: Vec<ReplayAnalyticsEvent>,
    pub timeline: Vec<ReplayPersonTimelineItem>,
}

#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct ReplayPersonTimelineItem {
    pub kind: String,
    pub title: String,
    pub timestamp: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    pub replay_anchor_ms: i64,
    pub detail: String,
}

#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct ReplayAnalyticsEvent {
    pub uuid: String,
    pub event: String,
    pub distinct_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
    pub created_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    pub replay_anchor_ms: i64,
    pub properties: Vec<ReplayEventProperty>,
}

#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct ReplayEventProperty {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReplaySnapshotRow {
    pub uuid: String,
    pub event: Option<String>,
    pub distinct_id: String,
    pub created_at: DateTime<Utc>,
    pub api_key: Option<String>,
    pub properties: Value,
}

impl TryFrom<Value> for ReplaySnapshotRow {
    type Error = ReplayError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        let object = value
            .as_object()
            .ok_or_else(|| ReplayError::InvalidRow("expected row object".to_string()))?;

        let uuid = string_field(object, "uuid")
            .ok_or_else(|| ReplayError::InvalidRow("missing uuid".to_string()))?;
        let event = string_field(object, "event");
        let distinct_id = string_field(object, "distinct_id")
            .ok_or_else(|| ReplayError::InvalidRow("missing distinct_id".to_string()))?;
        let created_at_raw = string_field(object, "created_at")
            .ok_or_else(|| ReplayError::InvalidRow("missing created_at".to_string()))?;
        let created_at = parse_datetime(&created_at_raw)?;
        let api_key = string_field(object, "api_key");
        let properties = object
            .get("properties")
            .map(parse_jsonish_value)
            .transpose()?
            .unwrap_or(Value::Null);

        Ok(Self {
            uuid,
            event,
            distinct_id,
            created_at,
            api_key,
            properties,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReplayEventRow {
    pub uuid: String,
    pub event: String,
    pub distinct_id: String,
    pub created_at: DateTime<Utc>,
    pub api_key: Option<String>,
    pub properties: Value,
}

impl TryFrom<Value> for ReplayEventRow {
    type Error = ReplayError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        let object = value
            .as_object()
            .ok_or_else(|| ReplayError::InvalidRow("expected row object".to_string()))?;

        let uuid = string_field(object, "uuid")
            .ok_or_else(|| ReplayError::InvalidRow("missing uuid".to_string()))?;
        let event = string_field(object, "event")
            .ok_or_else(|| ReplayError::InvalidRow("missing event".to_string()))?;
        let distinct_id = string_field(object, "distinct_id")
            .ok_or_else(|| ReplayError::InvalidRow("missing distinct_id".to_string()))?;
        let created_at_raw = string_field(object, "created_at")
            .ok_or_else(|| ReplayError::InvalidRow("missing created_at".to_string()))?;
        let created_at = parse_datetime(&created_at_raw)?;
        let api_key = string_field(object, "api_key");
        let properties = object
            .get("properties")
            .map(parse_jsonish_value)
            .transpose()?
            .unwrap_or(Value::Null);

        Ok(Self {
            uuid,
            event,
            distinct_id,
            created_at,
            api_key,
            properties,
        })
    }
}

#[derive(Debug)]
struct ExtractedReplayEvents {
    events: Vec<Value>,
    source_shape: String,
}

pub fn build_snapshot_rows_sql(table: &str, query: &ReplaySessionsQuery, limit: usize) -> String {
    let mut clauses = vec![format!(
        "event in ({}, {})",
        sql_string_literal(SNAPSHOT_EVENT),
        sql_string_literal(SNAPSHOT_ITEMS_EVENT)
    )];
    if let Some(api_key) = query.api_key.as_ref().filter(|value| !value.is_empty()) {
        clauses.push(format!("api_key = {}", sql_string_literal(api_key)));
    }
    if let Some(distinct_id) = query.distinct_id.as_ref().filter(|value| !value.is_empty()) {
        clauses.push(format!("distinct_id = {}", sql_string_literal(distinct_id)));
    }
    push_date_clauses(
        &mut clauses,
        query.date_from.as_deref(),
        query.date_to.as_deref(),
    );

    format!(
        "select uuid, event, distinct_id, created_at, properties, api_key from {table} where {} order by created_at desc limit {limit}",
        clauses.join(" and ")
    )
}

pub fn build_event_rows_sql(table: &str, query: &ReplayEventsQuery, limit: usize) -> String {
    let mut clauses = vec![format!(
        "event not in ({}, {})",
        sql_string_literal(SNAPSHOT_EVENT),
        sql_string_literal(SNAPSHOT_ITEMS_EVENT)
    )];
    if let Some(api_key) = query.api_key.as_ref().filter(|value| !value.is_empty()) {
        clauses.push(format!("api_key = {}", sql_string_literal(api_key)));
    }
    if let Some(distinct_id) = query.distinct_id.as_ref().filter(|value| !value.is_empty()) {
        clauses.push(format!("distinct_id = {}", sql_string_literal(distinct_id)));
    }
    if let Some(event_name) = query.event_name.as_ref().filter(|value| !value.is_empty()) {
        clauses.push(format!("event = {}", sql_string_literal(event_name)));
    }
    push_date_clauses(
        &mut clauses,
        query.date_from.as_deref(),
        query.date_to.as_deref(),
    );

    format!(
        "select uuid, event, distinct_id, created_at, properties, api_key from {table} where {} order by created_at desc limit {limit}",
        clauses.join(" and ")
    )
}

pub fn build_sessions_response(
    rows: Vec<ReplaySnapshotRow>,
    limit: usize,
) -> ReplaySessionsResponse {
    let mut sessions: HashMap<String, SessionAccumulator> = HashMap::new();

    for row in rows {
        let session_id = session_id_for_row(&row);
        let extracted = extract_rrweb_events(&row.properties);
        let stats = extracted
            .as_ref()
            .map(|value| replay_event_stats(&value.events))
            .unwrap_or_default();

        let entry = sessions
            .entry(session_id.clone())
            .or_insert_with(|| SessionAccumulator::new(session_id, row.clone()));
        entry.add(row, stats);
    }

    let mut summaries: Vec<ReplaySessionSummary> = sessions
        .into_values()
        .map(SessionAccumulator::finish)
        .collect();
    summaries.sort_by(|a, b| b.last_seen.cmp(&a.last_seen));
    summaries.truncate(limit);

    ReplaySessionsResponse {
        sessions: summaries,
    }
}

pub fn build_session_events_response(
    session_id: &str,
    rows: Vec<ReplaySnapshotRow>,
    replay_anchor_ms: Option<i64>,
) -> Result<ReplaySessionEventsResponse, ReplayError> {
    let mut events = Vec::new();
    let mut chunks = Vec::new();
    let mut matched_rows = Vec::new();

    for row in rows {
        if session_id_for_row(&row) != session_id {
            continue;
        }
        let extracted = extract_rrweb_events(&row.properties).unwrap_or(ExtractedReplayEvents {
            events: Vec::new(),
            source_shape: "unrecognized".to_string(),
        });
        chunks.push(ReplayChunkSummary {
            uuid: row.uuid.clone(),
            created_at: row.created_at,
            event_count: extracted.events.len(),
            source_shape: extracted.source_shape,
        });
        events.extend(extracted.events);
        matched_rows.push(row);
    }

    if matched_rows.is_empty() {
        return Err(ReplayError::SessionNotFound(session_id.to_string()));
    }

    events.sort_by_key(rrweb_timestamp);
    chunks.sort_by_key(|chunk| chunk.created_at);
    let activity = build_activity(&events);
    let replay_start_ms = events.iter().filter_map(rrweb_timestamp_opt).min();
    let replay_end_ms = events.iter().filter_map(rrweb_timestamp_opt).max();
    let replay_anchor_ms = replay_anchor_ms.or(Some(0));

    let session = build_sessions_response(matched_rows, 1)
        .sessions
        .into_iter()
        .next()
        .ok_or_else(|| ReplayError::SessionNotFound(session_id.to_string()))?;

    Ok(ReplaySessionEventsResponse {
        session,
        events,
        activity,
        chunks,
        replay_start_ms,
        replay_end_ms,
        replay_anchor_ms,
    })
}

pub fn build_events_response(rows: Vec<ReplayEventRow>, limit: usize) -> ReplayEventsResponse {
    let mut events = rows
        .into_iter()
        .map(ReplayAnalyticsEvent::from)
        .collect::<Vec<_>>();
    events.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    events.truncate(limit);
    ReplayEventsResponse { events }
}

pub fn build_funnel_response(
    rows: Vec<ReplayEventRow>,
    steps: Vec<String>,
    limit: usize,
) -> ReplayFunnelResponse {
    if steps.is_empty() {
        return ReplayFunnelResponse {
            steps,
            sessions: Vec::new(),
        };
    }

    let mut grouped: HashMap<String, Vec<ReplayAnalyticsEvent>> = HashMap::new();
    for row in rows {
        let event = ReplayAnalyticsEvent::from(row);
        let key = event
            .session_id
            .clone()
            .unwrap_or_else(|| format!("person:{}", event.distinct_id));
        grouped.entry(key).or_default().push(event);
    }

    let mut sessions = grouped
        .into_values()
        .filter_map(|mut events| {
            events.sort_by(|a, b| a.created_at.cmp(&b.created_at));
            funnel_session_from_events(&events, &steps)
        })
        .collect::<Vec<_>>();
    sessions.sort_by(|a, b| b.last_seen.cmp(&a.last_seen));
    sessions.truncate(limit);

    ReplayFunnelResponse { steps, sessions }
}

pub fn build_friction_response(
    rows: Vec<ReplaySnapshotRow>,
    signal_filter: Option<&str>,
    limit: usize,
) -> ReplayFrictionResponse {
    let mut grouped: HashMap<String, Vec<ReplaySnapshotRow>> = HashMap::new();
    for row in rows {
        grouped
            .entry(session_id_for_row(&row))
            .or_default()
            .push(row);
    }

    let normalized_signal_filter = signal_filter
        .filter(|value| !value.is_empty())
        .map(|value| value.to_ascii_lowercase());
    let mut sessions = Vec::new();
    for (session_id, rows) in grouped {
        let Ok(response) = build_session_events_response(&session_id, rows, None) else {
            continue;
        };
        let mut signals = detect_friction_signals(&response.events);
        if let Some(filter) = normalized_signal_filter.as_deref() {
            signals.retain(|signal| signal.kind == filter);
        }
        if signals.is_empty() {
            continue;
        }
        let score = signals.iter().map(friction_signal_weight).sum();
        sessions.push(ReplayFrictionSession {
            session: response.session,
            score,
            signals,
        });
    }

    sessions.sort_by(|a, b| {
        b.score
            .cmp(&a.score)
            .then_with(|| b.session.last_seen.cmp(&a.session.last_seen))
    });
    sessions.truncate(limit);
    ReplayFrictionResponse { sessions }
}

pub fn build_person_journey_response(
    distinct_id: Option<String>,
    sessions: Vec<ReplaySessionSummary>,
    events: Vec<ReplayAnalyticsEvent>,
    limit: usize,
) -> ReplayPersonJourneyResponse {
    let mut timeline = Vec::new();

    for session in &sessions {
        timeline.push(ReplayPersonTimelineItem {
            kind: "session".to_string(),
            title: "Replay session".to_string(),
            timestamp: session.first_seen,
            session_id: Some(session.session_id.clone()),
            url: session
                .first_url
                .clone()
                .or_else(|| session.last_url.clone()),
            replay_anchor_ms: 0,
            detail: format!(
                "{} rrweb events, {} chunks, {}ms",
                session.event_count, session.chunk_count, session.duration_ms
            ),
        });
    }

    for event in &events {
        timeline.push(ReplayPersonTimelineItem {
            kind: "event".to_string(),
            title: event.event.clone(),
            timestamp: event.created_at,
            session_id: event.session_id.clone(),
            url: event.url.clone(),
            replay_anchor_ms: event.replay_anchor_ms,
            detail: event_property_line(event),
        });
    }

    timeline.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
    timeline.truncate(limit);

    ReplayPersonJourneyResponse {
        distinct_id,
        sessions,
        events,
        timeline,
    }
}

pub fn rows_from_r2_sql_response(value: Value) -> Result<Vec<Value>, ReplayError> {
    if let Value::Array(rows) = value {
        return Ok(rows);
    }

    let candidates = [
        "/rows",
        "/data",
        "/result/rows",
        "/result/data",
        "/result/results",
        "/result/0/results",
        "/result/0/rows",
        "/result/0/data",
    ];

    for pointer in candidates {
        if let Some(Value::Array(rows)) = value.pointer(pointer) {
            return Ok(rows.clone());
        }
    }

    Err(ReplayError::MissingRows)
}

pub fn replay_ui_html() -> &'static str {
    include_str!("replay_ui.html")
}

impl From<ReplayEventRow> for ReplayAnalyticsEvent {
    fn from(row: ReplayEventRow) -> Self {
        let session_id = session_id_from_value(&row.properties);
        let url = event_url_from_properties(&row.properties);
        let properties = summarize_event_properties(&row.properties);

        Self {
            uuid: row.uuid,
            event: row.event,
            distinct_id: row.distinct_id,
            api_key: row.api_key,
            created_at: row.created_at,
            session_id,
            url,
            replay_anchor_ms: row.created_at.timestamp_millis(),
            properties,
        }
    }
}

fn funnel_session_from_events(
    events: &[ReplayAnalyticsEvent],
    steps: &[String],
) -> Option<ReplayFunnelSession> {
    let first = events.first()?;
    let last = events.last()?;
    let mut next_step = 0;
    let mut repeated_current_step = 0;
    let mut matched_steps = Vec::new();

    for event in events {
        if next_step < steps.len() && event.event == steps[next_step] {
            matched_steps.push(ReplayFunnelStepEvent {
                step_index: next_step + 1,
                event: event.event.clone(),
                created_at: event.created_at,
                session_id: event.session_id.clone(),
                url: event.url.clone(),
            });
            next_step += 1;
            continue;
        }

        if next_step > 0 && next_step < steps.len() && event.event == steps[next_step - 1] {
            repeated_current_step += 1;
        }
    }

    if next_step == 0 {
        return None;
    }

    let anchor = matched_steps
        .last()
        .map(|event| event.created_at.timestamp_millis())
        .unwrap_or_else(|| first.created_at.timestamp_millis());
    let status = if next_step == steps.len() {
        "converted"
    } else if repeated_current_step > 0 {
        "stuck"
    } else {
        "dropped"
    };

    Some(ReplayFunnelSession {
        distinct_id: first.distinct_id.clone(),
        api_key: first.api_key.clone(),
        session_id: first.session_id.clone(),
        status: status.to_string(),
        completed_steps: next_step,
        step_count: steps.len(),
        current_step: matched_steps.last().map(|event| event.event.clone()),
        drop_off_step: steps.get(next_step).cloned(),
        first_seen: first.created_at,
        last_seen: last.created_at,
        url: matched_steps
            .last()
            .and_then(|event| event.url.clone())
            .or_else(|| events.iter().find_map(|event| event.url.clone())),
        replay_anchor_ms: anchor,
        steps: matched_steps,
    })
}

fn detect_friction_signals(events: &[Value]) -> Vec<ReplayFrictionSignal> {
    let start = events
        .iter()
        .filter_map(rrweb_timestamp_opt)
        .min()
        .unwrap_or(0);
    let clicks = replay_clicks(events, start);
    let mut signals = Vec::new();

    if let Some(signal) = detect_rage_click(&clicks, start) {
        signals.push(signal);
    }
    if let Some(signal) = detect_dead_clicks(events, &clicks, start) {
        signals.push(signal);
    }
    if let Some(signal) = detect_repeated_navigation(events, start) {
        signals.push(signal);
    }
    if let Some(signal) = detect_long_idle(events, start) {
        signals.push(signal);
    }
    if let Some(signal) = detect_form_thrash(events, start) {
        signals.push(signal);
    }
    if let Some(signal) = detect_missed_cta(events, &clicks, start) {
        signals.push(signal);
    }

    signals.sort_by_key(|signal| signal.timestamp);
    signals
}

#[derive(Debug, Clone)]
struct ReplayClick {
    timestamp: i64,
    x: Option<i64>,
    y: Option<i64>,
    url: Option<String>,
}

#[derive(Debug, Clone)]
struct ReplayScroll {
    timestamp: i64,
    y: i64,
    url: Option<String>,
}

fn replay_clicks(events: &[Value], start: i64) -> Vec<ReplayClick> {
    events
        .iter()
        .filter(|event| rrweb_source(event) == Some(2))
        .filter(|event| {
            matches!(
                event.pointer("/data/type").and_then(Value::as_i64),
                Some(2 | 4)
            )
        })
        .filter_map(|event| {
            let timestamp = rrweb_timestamp_opt(event)?;
            Some(ReplayClick {
                timestamp,
                x: event.pointer("/data/x").and_then(Value::as_i64),
                y: event.pointer("/data/y").and_then(Value::as_i64),
                url: url_from_rrweb_event(event).or_else(|| current_url_at(events, timestamp)),
            })
        })
        .filter(|click| click.timestamp >= start)
        .collect()
}

fn detect_rage_click(clicks: &[ReplayClick], start: i64) -> Option<ReplayFrictionSignal> {
    for (index, first) in clicks.iter().enumerate() {
        let cluster = clicks
            .iter()
            .skip(index)
            .take_while(|click| click.timestamp.saturating_sub(first.timestamp) <= 2_000)
            .filter(|click| clicks_are_near(first, click, 25))
            .count();
        if cluster >= 3 {
            return Some(friction_signal(
                "rage_click",
                "Rage clicks",
                format!("{cluster} clicks inside 2s near the same point"),
                "high",
                cluster,
                first.timestamp,
                start,
                first.url.clone(),
            ));
        }
    }
    None
}

fn detect_dead_clicks(
    events: &[Value],
    clicks: &[ReplayClick],
    start: i64,
) -> Option<ReplayFrictionSignal> {
    let dead = clicks
        .iter()
        .filter(|click| {
            !events.iter().any(|event| {
                let timestamp = rrweb_timestamp(event);
                timestamp > click.timestamp
                    && timestamp.saturating_sub(click.timestamp) <= 1_500
                    && is_click_follow_up(event)
            })
        })
        .collect::<Vec<_>>();
    let first = dead.first()?;

    Some(friction_signal(
        "dead_click",
        "Dead clicks",
        format!(
            "{} {} had no DOM, navigation, or input follow-up",
            dead.len(),
            if dead.len() == 1 { "click" } else { "clicks" }
        ),
        if dead.len() >= 3 { "high" } else { "medium" },
        dead.len(),
        first.timestamp,
        start,
        first.url.clone(),
    ))
}

fn detect_repeated_navigation(events: &[Value], start: i64) -> Option<ReplayFrictionSignal> {
    let navigations = events
        .iter()
        .filter(|event| event.get("type").and_then(Value::as_i64) == Some(4))
        .filter_map(|event| {
            let timestamp = rrweb_timestamp_opt(event)?;
            let url = url_from_rrweb_event(event)?;
            Some((timestamp, url))
        })
        .collect::<Vec<_>>();

    let mut counts: HashMap<String, usize> = HashMap::new();
    for (_, url) in &navigations {
        *counts.entry(url.clone()).or_default() += 1;
    }
    if let Some((url, count)) = counts.into_iter().find(|(_, count)| *count >= 3) {
        let timestamp = navigations
            .iter()
            .find(|(_, candidate)| candidate == &url)
            .map(|(timestamp, _)| *timestamp)
            .unwrap_or(start);
        return Some(friction_signal(
            "repeated_navigation",
            "Repeated navigation",
            format!("{count} visits to the same URL"),
            "medium",
            count,
            timestamp,
            start,
            Some(url),
        ));
    }

    for window in navigations.windows(3) {
        if window[0].1 == window[2].1 && window[0].1 != window[1].1 {
            return Some(friction_signal(
                "repeated_navigation",
                "Back and forth navigation",
                "Returned to the same URL after one intervening page".to_string(),
                "medium",
                3,
                window[0].0,
                start,
                Some(window[0].1.clone()),
            ));
        }
    }

    None
}

fn detect_long_idle(events: &[Value], start: i64) -> Option<ReplayFrictionSignal> {
    let mut timestamps = events
        .iter()
        .filter_map(rrweb_timestamp_opt)
        .collect::<Vec<_>>();
    timestamps.sort_unstable();
    let (before, gap) = timestamps
        .windows(2)
        .map(|window| (window[0], window[1].saturating_sub(window[0])))
        .max_by_key(|(_, gap)| *gap)?;

    if gap < 30_000 {
        return None;
    }

    Some(friction_signal(
        "long_idle",
        "Long idle",
        format!("{}s gap before the next recorded action", gap / 1_000),
        "medium",
        1,
        before,
        start,
        current_url_at(events, before),
    ))
}

fn detect_form_thrash(events: &[Value], start: i64) -> Option<ReplayFrictionSignal> {
    let inputs = events
        .iter()
        .filter(|event| rrweb_source(event) == Some(5))
        .filter_map(rrweb_timestamp_opt)
        .collect::<Vec<_>>();

    for (index, first) in inputs.iter().enumerate() {
        let count = inputs
            .iter()
            .skip(index)
            .take_while(|timestamp| timestamp.saturating_sub(*first) <= 10_000)
            .count();
        if count >= 3 {
            return Some(friction_signal(
                "form_thrash",
                "Form thrash",
                format!("{count} input changes inside 10s"),
                "high",
                count,
                *first,
                start,
                current_url_at(events, *first),
            ));
        }
    }

    None
}

fn detect_missed_cta(
    events: &[Value],
    clicks: &[ReplayClick],
    start: i64,
) -> Option<ReplayFrictionSignal> {
    let deepest = events
        .iter()
        .filter(|event| rrweb_source(event) == Some(3))
        .filter_map(|event| {
            let timestamp = rrweb_timestamp_opt(event)?;
            let y = event.pointer("/data/y").and_then(Value::as_i64)?;
            Some(ReplayScroll {
                timestamp,
                y,
                url: current_url_at(events, timestamp),
            })
        })
        .max_by_key(|scroll| scroll.y)?;

    if deepest.y < 1_200
        || clicks
            .iter()
            .any(|click| click.timestamp > deepest.timestamp)
    {
        return None;
    }

    Some(friction_signal(
        "missed_cta",
        "Deep scroll without follow-up",
        format!("Scrolled to y={} with no later click", deepest.y),
        "low",
        1,
        deepest.timestamp,
        start,
        deepest.url,
    ))
}

fn friction_signal(
    kind: &str,
    label: &str,
    detail: String,
    severity: &str,
    count: usize,
    timestamp: i64,
    start: i64,
    url: Option<String>,
) -> ReplayFrictionSignal {
    ReplayFrictionSignal {
        kind: kind.to_string(),
        label: label.to_string(),
        detail,
        severity: severity.to_string(),
        count,
        timestamp,
        replay_anchor_ms: timestamp.saturating_sub(start),
        url,
    }
}

fn friction_signal_weight(signal: &ReplayFrictionSignal) -> usize {
    match signal.kind.as_str() {
        "rage_click" => 5,
        "form_thrash" => 4,
        "repeated_navigation" => 3,
        "dead_click" | "long_idle" => 2,
        "missed_cta" => 1,
        _ => 1,
    }
}

fn clicks_are_near(left: &ReplayClick, right: &ReplayClick, radius: i64) -> bool {
    match (left.x, left.y, right.x, right.y) {
        (Some(left_x), Some(left_y), Some(right_x), Some(right_y)) => {
            left_x.saturating_sub(right_x).abs() <= radius
                && left_y.saturating_sub(right_y).abs() <= radius
        }
        _ => true,
    }
}

fn is_click_follow_up(event: &Value) -> bool {
    match event.get("type").and_then(Value::as_i64) {
        Some(2 | 4) => true,
        Some(3) => matches!(rrweb_source(event), Some(0 | 5)),
        _ => false,
    }
}

fn rrweb_source(event: &Value) -> Option<i64> {
    if event.get("type").and_then(Value::as_i64) != Some(3) {
        return None;
    }
    event.pointer("/data/source").and_then(Value::as_i64)
}

fn current_url_at(events: &[Value], timestamp: i64) -> Option<String> {
    events
        .iter()
        .filter(|event| rrweb_timestamp(event) <= timestamp)
        .filter_map(url_from_rrweb_event)
        .last()
}

fn event_property_line(event: &ReplayAnalyticsEvent) -> String {
    if event.properties.is_empty() {
        return event
            .url
            .clone()
            .or_else(|| event.session_id.clone())
            .unwrap_or_else(|| event.distinct_id.clone());
    }

    event
        .properties
        .iter()
        .take(3)
        .map(|property| format!("{}={}", property.key, property.value))
        .collect::<Vec<_>>()
        .join(", ")
}

fn split_steps(value: Option<&str>) -> Vec<String> {
    value
        .unwrap_or_default()
        .split(|character| matches!(character, ',' | '\n' | '>'))
        .map(str::trim)
        .filter(|step| !step.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn retain_snapshot_rows_for_session(rows: &mut Vec<ReplaySnapshotRow>, session_id: Option<&str>) {
    if let Some(session_id) = session_id.filter(|value| !value.is_empty()) {
        rows.retain(|row| session_id_for_row(row) == session_id);
    }
}

fn apply_session_summary_filters(
    sessions: &mut Vec<ReplaySessionSummary>,
    query: &ReplaySessionsQuery,
) {
    if let Some(url) = query.url.as_ref().filter(|value| !value.is_empty()) {
        sessions.retain(|session| {
            session
                .first_url
                .as_ref()
                .or(session.last_url.as_ref())
                .map(|candidate| contains_case_insensitive(candidate, url))
                .unwrap_or(false)
        });
    }

    if let Some(min_duration_secs) = query.min_duration_secs {
        sessions.retain(|session| session.duration_ms >= min_duration_secs.saturating_mul(1_000));
    }
    if let Some(max_duration_secs) = query.max_duration_secs {
        sessions.retain(|session| session.duration_ms <= max_duration_secs.saturating_mul(1_000));
    }
    if let Some(min_events) = query.min_events {
        sessions.retain(|session| session.event_count >= min_events);
    }
    if let Some(max_events) = query.max_events {
        sessions.retain(|session| session.event_count <= max_events);
    }
}

#[derive(Debug, Default)]
struct MatchingSessionKeys {
    session_ids: HashSet<String>,
    distinct_ids: HashSet<String>,
}

fn matching_session_keys_from_events(rows: Vec<ReplayEventRow>) -> MatchingSessionKeys {
    let mut keys = MatchingSessionKeys::default();
    for row in rows {
        if let Some(session_id) = session_id_from_value(&row.properties) {
            keys.session_ids.insert(session_id);
        }
        keys.distinct_ids.insert(row.distinct_id);
    }
    keys
}

fn extract_rrweb_events(value: &Value) -> Option<ExtractedReplayEvents> {
    let parsed = parse_jsonish_value(value).ok()?;
    let mut candidates = Vec::new();
    collect_event_candidates(&parsed, &mut candidates);

    if let Some(events) = candidates.into_iter().find(|events| !events.is_empty()) {
        return Some(ExtractedReplayEvents {
            events,
            source_shape: "json-events".to_string(),
        });
    }

    let chunk = parsed
        .pointer("/data/chunk")
        .or_else(|| parsed.pointer("/chunk"))
        .and_then(Value::as_str)?;
    let compression = parsed
        .pointer("/data/compression")
        .or_else(|| parsed.pointer("/compression"))
        .and_then(Value::as_str);
    let decoded = decode_chunk_payload(chunk, compression)?;
    let mut chunk_candidates = Vec::new();
    collect_event_candidates(&decoded, &mut chunk_candidates);

    chunk_candidates
        .into_iter()
        .find(|events| !events.is_empty())
        .map(|events| ExtractedReplayEvents {
            events,
            source_shape: "base64-chunk".to_string(),
        })
}

fn collect_event_candidates(value: &Value, candidates: &mut Vec<Vec<Value>>) {
    if is_rrweb_event(value) {
        candidates.push(vec![value.clone()]);
    }

    if let Value::Array(items) = value {
        if items.iter().all(is_rrweb_event) {
            candidates.push(items.clone());
        }
    }

    for pointer in [
        "/$snapshot_items",
        "/$snapshot_data",
        "/events",
        "/snapshots",
        "/batch",
        "/data",
        "/data/$snapshot_items",
        "/data/$snapshot_data",
        "/data/events",
        "/data/snapshots",
        "/data/batch",
        "/payload/events",
    ] {
        if let Some(candidate) = value.pointer(pointer) {
            if let Value::Array(items) = candidate {
                if items.iter().all(is_rrweb_event) {
                    candidates.push(items.clone());
                }
            } else if is_rrweb_event(candidate) {
                candidates.push(vec![candidate.clone()]);
            }
        }
    }
}

fn is_replay_snapshot_event(event: &str) -> bool {
    matches!(event, SNAPSHOT_EVENT | SNAPSHOT_ITEMS_EVENT)
}

fn decode_chunk_payload(chunk: &str, compression: Option<&str>) -> Option<Value> {
    let bytes = BASE64_STANDARD
        .decode(chunk)
        .ok()
        .or_else(|| Some(chunk.as_bytes().to_vec()))?;
    let text = if compression
        .map(|value| value.to_ascii_lowercase().contains("gzip"))
        .unwrap_or(false)
        || bytes.starts_with(&[0x1f, 0x8b])
    {
        let mut decoder = GzDecoder::new(bytes.as_slice());
        let mut decoded = String::new();
        decoder.read_to_string(&mut decoded).ok()?;
        decoded
    } else {
        String::from_utf8(bytes).ok()?
    };

    serde_json::from_str(&text).ok()
}

fn is_rrweb_event(value: &Value) -> bool {
    value
        .as_object()
        .map(|object| {
            object.get("type").and_then(Value::as_i64).is_some()
                && object.get("timestamp").and_then(Value::as_i64).is_some()
        })
        .unwrap_or(false)
}

fn session_id_for_row(row: &ReplaySnapshotRow) -> String {
    session_id_from_value(&row.properties).unwrap_or_else(|| row.distinct_id.clone())
}

fn session_id_from_value(value: &Value) -> Option<String> {
    let parsed = parse_jsonish_value(value).ok()?;
    for pointer in [
        "/session_id",
        "/sessionId",
        "/$session_id",
        "/recording_id",
        "/session_recording_id",
        "/data/session_id",
        "/data/sessionId",
        "/data/$session_id",
        "/data/metadata/session_id",
        "/data/metadata/sessionId",
        "/data/metadata/$session_id",
    ] {
        if let Some(session_id) = parsed.pointer(pointer).and_then(Value::as_str) {
            if !session_id.is_empty() {
                return Some(session_id.to_string());
            }
        }
    }
    None
}

fn first_url_from_events(events: &[Value]) -> Option<String> {
    for event in events {
        if let Some(value) = url_from_rrweb_event(event) {
            return Some(value);
        }
    }
    None
}

fn last_url_from_events(events: &[Value]) -> Option<String> {
    events.iter().rev().find_map(url_from_rrweb_event)
}

fn url_from_rrweb_event(event: &Value) -> Option<String> {
    for pointer in [
        "/data/href",
        "/data/source",
        "/data/attributes/href",
        "/data/node/root/childNodes/0/attributes/href",
    ] {
        if let Some(value) = event.pointer(pointer).and_then(Value::as_str) {
            if value.starts_with("http://") || value.starts_with("https://") {
                return Some(value.to_string());
            }
        }
    }
    None
}

#[derive(Debug, Default)]
struct ReplayEventStats {
    event_count: usize,
    first_url: Option<String>,
    last_url: Option<String>,
    start_ms: Option<i64>,
    end_ms: Option<i64>,
}

fn replay_event_stats(events: &[Value]) -> ReplayEventStats {
    ReplayEventStats {
        event_count: events.len(),
        first_url: first_url_from_events(events),
        last_url: last_url_from_events(events),
        start_ms: events.iter().filter_map(rrweb_timestamp_opt).min(),
        end_ms: events.iter().filter_map(rrweb_timestamp_opt).max(),
    }
}

fn rrweb_timestamp(value: &Value) -> i64 {
    value.get("timestamp").and_then(Value::as_i64).unwrap_or(0)
}

fn rrweb_timestamp_opt(value: &Value) -> Option<i64> {
    value.get("timestamp").and_then(Value::as_i64)
}

fn build_activity(events: &[Value]) -> Vec<ReplayActivityItem> {
    let start = events
        .iter()
        .filter_map(rrweb_timestamp_opt)
        .min()
        .unwrap_or(0);
    events
        .iter()
        .enumerate()
        .filter_map(|(index, event)| activity_for_event(index, event, start))
        .collect()
}

fn activity_for_event(index: usize, event: &Value, start: i64) -> Option<ReplayActivityItem> {
    let event_type = event.get("type").and_then(Value::as_i64)?;
    let timestamp = rrweb_timestamp_opt(event)?;
    let url = url_from_rrweb_event(event);
    let (kind, label, detail) = match event_type {
        2 => ("snapshot", "DOM snapshot".to_string(), None),
        4 => (
            "navigation",
            "Page context".to_string(),
            url.clone().or_else(|| string_pointer(event, "/data/href")),
        ),
        5 => (
            "custom",
            string_pointer(event, "/data/tag").unwrap_or_else(|| "Custom marker".to_string()),
            string_pointer(event, "/data/payload"),
        ),
        3 => incremental_activity(event)?,
        _ => return None,
    };

    Some(ReplayActivityItem {
        id: format!("rrweb-{index}"),
        timestamp,
        offset_ms: timestamp.saturating_sub(start),
        kind: kind.to_string(),
        label,
        detail,
        url,
        replay_anchor_ms: timestamp.saturating_sub(start),
    })
}

fn incremental_activity(event: &Value) -> Option<(&'static str, String, Option<String>)> {
    let source = event.pointer("/data/source").and_then(Value::as_i64)?;
    match source {
        0 => Some(("dom", "DOM changed".to_string(), None)),
        2 => Some(mouse_activity(event)),
        3 => Some((
            "scroll",
            "Scrolled".to_string(),
            xy_detail(event.pointer("/data/x"), event.pointer("/data/y")),
        )),
        4 => Some((
            "viewport",
            "Viewport resized".to_string(),
            size_detail(event.pointer("/data/width"), event.pointer("/data/height")),
        )),
        5 => Some(("input", "Input changed".to_string(), None)),
        7 => Some(("media", "Media interaction".to_string(), None)),
        11 => Some(("technical", "Browser log".to_string(), None)),
        _ => None,
    }
}

fn mouse_activity(event: &Value) -> (&'static str, String, Option<String>) {
    let label = match event.pointer("/data/type").and_then(Value::as_i64) {
        Some(0) => "Mouse up",
        Some(1) => "Mouse down",
        Some(2) => "Click",
        Some(3) => "Context menu",
        Some(4) => "Double click",
        Some(5) => "Focus",
        Some(6) => "Blur",
        Some(7) => "Touch start",
        Some(9) => "Touch end",
        _ => "Mouse interaction",
    };

    (
        "interaction",
        label.to_string(),
        xy_detail(event.pointer("/data/x"), event.pointer("/data/y")),
    )
}

fn xy_detail(x: Option<&Value>, y: Option<&Value>) -> Option<String> {
    Some(format!(
        "{}, {}",
        x.and_then(Value::as_i64)?,
        y.and_then(Value::as_i64)?
    ))
}

fn size_detail(width: Option<&Value>, height: Option<&Value>) -> Option<String> {
    Some(format!(
        "{} x {}",
        width.and_then(Value::as_i64)?,
        height.and_then(Value::as_i64)?
    ))
}

fn event_url_from_properties(properties: &Value) -> Option<String> {
    for pointer in [
        "/$current_url",
        "/current_url",
        "/url",
        "/href",
        "/page_url",
        "/properties/$current_url",
        "/properties/current_url",
        "/properties/url",
        "/properties/href",
    ] {
        if let Some(value) = properties.pointer(pointer).and_then(Value::as_str) {
            if value.starts_with("http://")
                || value.starts_with("https://")
                || value.starts_with('/')
            {
                return Some(value.to_string());
            }
        }
    }
    None
}

fn summarize_event_properties(properties: &Value) -> Vec<ReplayEventProperty> {
    let Some(object) = properties.as_object() else {
        return Vec::new();
    };
    let preferred = [
        "$current_url",
        "current_url",
        "url",
        "path",
        "pathname",
        "$browser",
        "$device_type",
        "$os",
    ];
    let mut summary = Vec::new();
    let mut seen = HashSet::new();

    for key in preferred {
        if let Some(value) = object.get(key).and_then(display_property_value) {
            seen.insert(key.to_string());
            summary.push(ReplayEventProperty {
                key: key.to_string(),
                value,
            });
        }
    }

    for (key, value) in object {
        if summary.len() >= 6 {
            break;
        }
        if seen.contains(key) || key.starts_with('$') {
            continue;
        }
        if let Some(value) = display_property_value(value) {
            summary.push(ReplayEventProperty {
                key: key.clone(),
                value,
            });
        }
    }

    summary
}

fn display_property_value(value: &Value) -> Option<String> {
    match value {
        Value::String(value) if !value.is_empty() => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

fn string_pointer(value: &Value, pointer: &str) -> Option<String> {
    value.pointer(pointer).and_then(|value| match value {
        Value::String(value) if !value.is_empty() => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    })
}

fn parse_jsonish_value(value: &Value) -> Result<Value, ReplayError> {
    match value {
        Value::String(raw) => serde_json::from_str(raw)
            .map_err(|err| ReplayError::InvalidRow(format!("invalid JSON string: {err}"))),
        other => Ok(other.clone()),
    }
}

fn parse_datetime(value: &str) -> Result<DateTime<Utc>, ReplayError> {
    DateTime::parse_from_rfc3339(value)
        .map(|dt| dt.with_timezone(&Utc))
        .or_else(|_| {
            DateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f %z")
                .map(|dt| dt.with_timezone(&Utc))
        })
        .map_err(|err| ReplayError::InvalidRow(format!("invalid created_at `{value}`: {err}")))
}

fn string_field(object: &Map<String, Value>, key: &str) -> Option<String> {
    object.get(key).and_then(|value| match value {
        Value::String(value) if !value.is_empty() => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    })
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn push_date_clauses(clauses: &mut Vec<String>, date_from: Option<&str>, date_to: Option<&str>) {
    if let Some(date_from) = date_from.filter(|value| !value.is_empty()) {
        clauses.push(format!("created_at >= {}", sql_string_literal(date_from)));
    }
    if let Some(date_to) = date_to.filter(|value| !value.is_empty()) {
        clauses.push(format!("created_at <= {}", sql_string_literal(date_to)));
    }
}

fn contains_case_insensitive(value: &str, needle: &str) -> bool {
    value
        .to_ascii_lowercase()
        .contains(&needle.to_ascii_lowercase())
}

fn validate_sql_identifier(value: &str) -> Result<(), ReplayConfigError> {
    if value
        .split('.')
        .all(|part| !part.is_empty() && part.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'))
    {
        Ok(())
    } else {
        Err(ReplayConfigError::InvalidEventsTable {
            value: value.to_string(),
        })
    }
}

#[derive(Debug)]
struct SessionAccumulator {
    session_id: String,
    distinct_id: String,
    api_key: Option<String>,
    first_seen: DateTime<Utc>,
    last_seen: DateTime<Utc>,
    replay_start_ms: Option<i64>,
    replay_end_ms: Option<i64>,
    chunk_count: usize,
    event_count: usize,
    first_url: Option<String>,
    last_url: Option<String>,
}

impl SessionAccumulator {
    fn new(session_id: String, row: ReplaySnapshotRow) -> Self {
        Self {
            session_id,
            distinct_id: row.distinct_id,
            api_key: row.api_key,
            first_seen: row.created_at,
            last_seen: row.created_at,
            replay_start_ms: None,
            replay_end_ms: None,
            chunk_count: 0,
            event_count: 0,
            first_url: None,
            last_url: None,
        }
    }

    fn add(&mut self, row: ReplaySnapshotRow, stats: ReplayEventStats) {
        self.first_seen = self.first_seen.min(row.created_at);
        self.last_seen = self.last_seen.max(row.created_at);
        self.chunk_count += 1;
        self.event_count += stats.event_count;
        self.replay_start_ms = min_optional_i64(self.replay_start_ms, stats.start_ms);
        self.replay_end_ms = max_optional_i64(self.replay_end_ms, stats.end_ms);
        if self.api_key.is_none() {
            self.api_key = row.api_key;
        }
        if self.first_url.is_none() {
            self.first_url = stats.first_url;
        }
        if stats.last_url.is_some() {
            self.last_url = stats.last_url;
        }
    }

    fn finish(self) -> ReplaySessionSummary {
        let duration_ms = match (self.replay_start_ms, self.replay_end_ms) {
            (Some(start), Some(end)) => end.saturating_sub(start),
            _ => self
                .last_seen
                .signed_duration_since(self.first_seen)
                .num_milliseconds()
                .max(0),
        };
        ReplaySessionSummary {
            session_id: self.session_id,
            distinct_id: self.distinct_id,
            api_key: self.api_key,
            first_seen: self.first_seen,
            last_seen: self.last_seen,
            duration_ms,
            chunk_count: self.chunk_count,
            event_count: self.event_count,
            first_url: self.first_url,
            last_url: self.last_url,
        }
    }
}

fn min_optional_i64(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

fn max_optional_i64(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn row(uuid: &str, session_id: &str, distinct_id: &str, timestamp: i64) -> ReplaySnapshotRow {
        ReplaySnapshotRow {
            uuid: uuid.to_string(),
            event: Some("$snapshot".to_string()),
            distinct_id: distinct_id.to_string(),
            created_at: Utc.timestamp_millis_opt(timestamp).unwrap(),
            api_key: Some("phc_test".to_string()),
            properties: json!({
                "session_id": session_id,
                "events": [
                    { "type": 4, "timestamp": timestamp, "data": { "href": "https://app.test/start" } }
                ]
            }),
        }
    }

    fn row_with_events(
        uuid: &str,
        session_id: &str,
        distinct_id: &str,
        events: Vec<Value>,
    ) -> ReplaySnapshotRow {
        ReplaySnapshotRow {
            uuid: uuid.to_string(),
            event: Some("$snapshot".to_string()),
            distinct_id: distinct_id.to_string(),
            created_at: Utc.timestamp_millis_opt(1_000).unwrap(),
            api_key: Some("phc_test".to_string()),
            properties: json!({
                "session_id": session_id,
                "events": events,
            }),
        }
    }

    fn snapshot_items_row(
        uuid: &str,
        session_id: &str,
        distinct_id: &str,
        events: Vec<Value>,
    ) -> ReplaySnapshotRow {
        ReplaySnapshotRow {
            uuid: uuid.to_string(),
            event: Some("$snapshot_items".to_string()),
            distinct_id: distinct_id.to_string(),
            created_at: Utc.timestamp_millis_opt(1_000).unwrap(),
            api_key: Some("phc_test".to_string()),
            properties: json!({
                "$session_id": session_id,
                "$snapshot_items": events,
            }),
        }
    }

    fn analytics_row(
        uuid: &str,
        event: &str,
        session_id: &str,
        distinct_id: &str,
        timestamp: i64,
    ) -> ReplayEventRow {
        ReplayEventRow {
            uuid: uuid.to_string(),
            event: event.to_string(),
            distinct_id: distinct_id.to_string(),
            created_at: Utc.timestamp_millis_opt(timestamp).unwrap(),
            api_key: Some("phc_test".to_string()),
            properties: json!({
                "$session_id": session_id,
                "$current_url": "https://app.test/pricing",
            }),
        }
    }

    #[test]
    fn builds_filtered_snapshot_sql_with_escaped_literals() {
        let sql = build_snapshot_rows_sql(
            "default.hogflare_events",
            &ReplaySessionsQuery {
                api_key: Some("phc_o'hare".to_string()),
                distinct_id: Some("user-1".to_string()),
                session_id: None,
                url: None,
                event_name: None,
                date_from: None,
                date_to: None,
                min_duration_secs: None,
                max_duration_secs: None,
                min_events: None,
                max_events: None,
                limit: None,
            },
            100,
        );

        assert_eq!(
            sql,
            "select uuid, event, distinct_id, created_at, properties, api_key from default.hogflare_events where event in ('$snapshot', '$snapshot_items') and api_key = 'phc_o''hare' and distinct_id = 'user-1' order by created_at desc limit 100"
        );
    }

    #[test]
    fn builds_filtered_event_sql_with_date_range() {
        let sql = build_event_rows_sql(
            "default.hogflare_events",
            &ReplayEventsQuery {
                api_key: Some("phc_test".to_string()),
                distinct_id: Some("user-1".to_string()),
                session_id: None,
                event_name: Some("Checkout Started".to_string()),
                url: None,
                date_from: Some("2026-05-22T00:00:00Z".to_string()),
                date_to: Some("2026-05-23T00:00:00Z".to_string()),
                limit: None,
            },
            50,
        );

        assert_eq!(
            sql,
            "select uuid, event, distinct_id, created_at, properties, api_key from default.hogflare_events where event not in ('$snapshot', '$snapshot_items') and api_key = 'phc_test' and distinct_id = 'user-1' and event = 'Checkout Started' and created_at >= '2026-05-22T00:00:00Z' and created_at <= '2026-05-23T00:00:00Z' order by created_at desc limit 50"
        );
    }

    #[test]
    fn extracts_sessions_from_rows() {
        let response = build_sessions_response(
            vec![
                row("chunk-2", "session-a", "user-a", 2_000),
                row("chunk-1", "session-a", "user-a", 1_000),
                row("chunk-3", "session-b", "user-b", 3_000),
            ],
            10,
        );

        assert_eq!(response.sessions.len(), 2);
        assert_eq!(response.sessions[0].session_id, "session-b");
        assert_eq!(response.sessions[1].session_id, "session-a");
        assert_eq!(response.sessions[1].chunk_count, 2);
        assert_eq!(response.sessions[1].event_count, 2);
        assert_eq!(
            response.sessions[1].first_url.as_deref(),
            Some("https://app.test/start")
        );
    }

    #[test]
    fn applies_session_summary_filters() {
        let mut response = build_sessions_response(
            vec![
                row("chunk-2", "session-a", "user-a", 2_000),
                row("chunk-1", "session-a", "user-a", 1_000),
                row("chunk-3", "session-b", "user-b", 3_000),
            ],
            10,
        );

        apply_session_summary_filters(
            &mut response.sessions,
            &ReplaySessionsQuery {
                api_key: None,
                distinct_id: None,
                session_id: None,
                url: Some("app.test/start".to_string()),
                event_name: None,
                date_from: None,
                date_to: None,
                min_duration_secs: Some(1),
                max_duration_secs: Some(1),
                min_events: Some(2),
                max_events: Some(2),
                limit: None,
            },
        );

        assert_eq!(response.sessions.len(), 1);
        assert_eq!(response.sessions[0].session_id, "session-a");
    }

    #[test]
    fn extracts_sorted_rrweb_events_from_session_chunks() {
        let response = build_session_events_response(
            "session-a",
            vec![
                row("chunk-2", "session-a", "user-a", 2_000),
                row("chunk-1", "session-a", "user-a", 1_000),
                row("chunk-other", "session-b", "user-b", 500),
            ],
            None,
        )
        .unwrap();

        assert_eq!(response.session.session_id, "session-a");
        assert_eq!(response.chunks.len(), 2);
        assert_eq!(response.events.len(), 2);
        assert_eq!(response.activity.len(), 2);
        assert_eq!(response.replay_start_ms, Some(1_000));
        assert_eq!(response.replay_end_ms, Some(2_000));
        assert_eq!(response.events[0]["timestamp"], 1_000);
        assert_eq!(response.events[1]["timestamp"], 2_000);
    }

    #[test]
    fn extracts_normalized_snapshot_items_rows() {
        let response = build_session_events_response(
            "session-normalized",
            vec![snapshot_items_row(
                "chunk-normalized",
                "session-normalized",
                "user-normalized",
                vec![json!({ "type": 4, "timestamp": 1_500, "data": { "href": "https://app.test/pricing" } })],
            )],
            None,
        )
        .unwrap();

        assert_eq!(response.session.session_id, "session-normalized");
        assert_eq!(response.session.event_count, 1);
        assert_eq!(response.events.len(), 1);
        assert_eq!(response.events[0]["timestamp"], 1_500);
        assert_eq!(
            response.session.first_url.as_deref(),
            Some("https://app.test/pricing")
        );
    }

    #[test]
    fn builds_analytics_event_response_without_raw_properties() {
        let response = build_events_response(
            vec![ReplayEventRow {
                uuid: "event-1".to_string(),
                event: "Checkout Started".to_string(),
                distinct_id: "user-a".to_string(),
                created_at: Utc.timestamp_millis_opt(2_000).unwrap(),
                api_key: Some("phc_test".to_string()),
                properties: json!({
                    "$session_id": "session-a",
                    "$current_url": "https://app.test/checkout",
                    "plan": "pro",
                    "nested": { "ignored": true }
                }),
            }],
            10,
        );

        assert_eq!(response.events.len(), 1);
        assert_eq!(response.events[0].session_id.as_deref(), Some("session-a"));
        assert_eq!(
            response.events[0].url.as_deref(),
            Some("https://app.test/checkout")
        );
        assert!(response.events[0]
            .properties
            .iter()
            .any(|property| property.key == "plan" && property.value == "pro"));
        assert!(!response.events[0]
            .properties
            .iter()
            .any(|property| property.key == "nested"));
    }

    #[test]
    fn classifies_funnel_sessions() {
        let response = build_funnel_response(
            vec![
                analytics_row("a1", "Viewed Pricing", "converted", "user-a", 1_000),
                analytics_row("a2", "Checkout Started", "converted", "user-a", 2_000),
                analytics_row("a3", "Paid", "converted", "user-a", 3_000),
                analytics_row("b1", "Viewed Pricing", "stuck", "user-b", 4_000),
                analytics_row("b2", "Viewed Pricing", "stuck", "user-b", 5_000),
                analytics_row("c1", "Viewed Pricing", "dropped", "user-c", 6_000),
                analytics_row("d1", "Signed Up", "ignored", "user-d", 7_000),
            ],
            vec![
                "Viewed Pricing".to_string(),
                "Checkout Started".to_string(),
                "Paid".to_string(),
            ],
            10,
        );

        assert_eq!(response.sessions.len(), 3);
        let converted = response
            .sessions
            .iter()
            .find(|session| session.session_id.as_deref() == Some("converted"))
            .unwrap();
        assert_eq!(converted.status, "converted");
        assert_eq!(converted.completed_steps, 3);
        assert_eq!(converted.drop_off_step, None);

        let stuck = response
            .sessions
            .iter()
            .find(|session| session.session_id.as_deref() == Some("stuck"))
            .unwrap();
        assert_eq!(stuck.status, "stuck");
        assert_eq!(stuck.completed_steps, 1);
        assert_eq!(stuck.drop_off_step.as_deref(), Some("Checkout Started"));

        let dropped = response
            .sessions
            .iter()
            .find(|session| session.session_id.as_deref() == Some("dropped"))
            .unwrap();
        assert_eq!(dropped.status, "dropped");
        assert_eq!(dropped.completed_steps, 1);
    }

    #[test]
    fn computes_friction_signals_from_rrweb_events() {
        let response = build_friction_response(
            vec![row_with_events(
                "chunk-friction",
                "session-friction",
                "user-friction",
                vec![
                    json!({ "type": 4, "timestamp": 1_000, "data": { "href": "https://app.test/pricing" } }),
                    json!({ "type": 3, "timestamp": 1_100, "data": { "source": 2, "type": 2, "x": 42, "y": 52 } }),
                    json!({ "type": 3, "timestamp": 1_700, "data": { "source": 2, "type": 2, "x": 44, "y": 50 } }),
                    json!({ "type": 3, "timestamp": 2_200, "data": { "source": 2, "type": 2, "x": 43, "y": 51 } }),
                    json!({ "type": 3, "timestamp": 3_000, "data": { "source": 5 } }),
                    json!({ "type": 3, "timestamp": 3_700, "data": { "source": 5 } }),
                    json!({ "type": 3, "timestamp": 4_200, "data": { "source": 5 } }),
                    json!({ "type": 3, "timestamp": 40_000, "data": { "source": 3, "x": 0, "y": 1_600 } }),
                    json!({ "type": 4, "timestamp": 45_000, "data": { "href": "https://app.test/help" } }),
                    json!({ "type": 4, "timestamp": 46_000, "data": { "href": "https://app.test/pricing" } }),
                    json!({ "type": 4, "timestamp": 47_000, "data": { "href": "https://app.test/help" } }),
                ],
            )],
            None,
            10,
        );

        assert_eq!(response.sessions.len(), 1);
        let kinds = response.sessions[0]
            .signals
            .iter()
            .map(|signal| signal.kind.as_str())
            .collect::<HashSet<_>>();
        assert!(kinds.contains("rage_click"));
        assert!(kinds.contains("form_thrash"));
        assert!(kinds.contains("long_idle"));
        assert!(kinds.contains("repeated_navigation"));
        assert!(kinds.contains("missed_cta"));
        assert!(response.sessions[0].score >= 10);
    }

    #[test]
    fn builds_person_journey_without_raw_event_properties() {
        let sessions =
            build_sessions_response(vec![row("chunk-1", "session-a", "user-a", 1_000)], 10)
                .sessions;
        let events = build_events_response(
            vec![ReplayEventRow {
                uuid: "event-1".to_string(),
                event: "Checkout Started".to_string(),
                distinct_id: "user-a".to_string(),
                created_at: Utc.timestamp_millis_opt(2_000).unwrap(),
                api_key: Some("phc_test".to_string()),
                properties: json!({
                    "$session_id": "session-a",
                    "$current_url": "https://app.test/checkout",
                    "plan": "pro",
                    "raw_object": { "hidden": true }
                }),
            }],
            10,
        )
        .events;

        let response =
            build_person_journey_response(Some("user-a".to_string()), sessions, events, 10);

        assert_eq!(response.distinct_id.as_deref(), Some("user-a"));
        assert_eq!(response.sessions.len(), 1);
        assert_eq!(response.events.len(), 1);
        assert_eq!(response.timeline.len(), 2);
        assert!(response
            .timeline
            .iter()
            .any(|item| item.kind == "event" && item.detail.contains("plan=pro")));
        assert!(!response
            .timeline
            .iter()
            .any(|item| item.detail.contains("raw_object")));
    }

    #[test]
    fn extracts_events_from_base64_gzip_chunk() {
        let encoded = {
            use flate2::{write::GzEncoder, Compression};
            use std::io::Write;

            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder
                .write_all(br#"[{"type":4,"timestamp":1000,"data":{}}]"#)
                .unwrap();
            BASE64_STANDARD.encode(encoder.finish().unwrap())
        };

        let extracted = extract_rrweb_events(&json!({
            "data": {
                "chunk": encoded,
                "compression": "gzip"
            }
        }))
        .unwrap();

        assert_eq!(extracted.source_shape, "base64-chunk");
        assert_eq!(extracted.events.len(), 1);
        assert_eq!(extracted.events[0]["type"], 4);
    }

    #[test]
    fn reads_rows_from_common_r2_sql_response_shapes() {
        let row = json!({
            "uuid": "row-1",
            "distinct_id": "user-1",
            "created_at": "2026-05-22T10:00:00Z",
            "properties": "{}"
        });

        assert_eq!(
            rows_from_r2_sql_response(json!({ "result": { "rows": [row.clone()] } })).unwrap(),
            vec![row.clone()]
        );
        assert_eq!(
            rows_from_r2_sql_response(json!({ "data": [row.clone()] })).unwrap(),
            vec![row]
        );
    }

    #[test]
    fn validates_table_identifier() {
        assert!(validate_sql_identifier("default.hogflare_events").is_ok());
        assert!(validate_sql_identifier("default.hogflare_events;drop").is_err());
    }
}
