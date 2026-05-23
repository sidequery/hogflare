#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;
#[cfg(not(target_arch = "wasm32"))]
use std::process::Stdio;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
#[cfg(not(target_arch = "wasm32"))]
use serde_json::{json, Value};
use thiserror::Error;

#[cfg(not(target_arch = "wasm32"))]
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::Mutex,
};

#[derive(Debug, Clone)]
#[cfg_attr(target_arch = "wasm32", allow(dead_code))]
pub struct ProductAnalyticsConfig {
    account_id: String,
    bucket_name: String,
    auth_token: String,
    events_table: String,
}

impl ProductAnalyticsConfig {
    pub fn new(
        account_id: String,
        bucket_name: String,
        auth_token: String,
        events_table: Option<String>,
    ) -> Self {
        Self {
            account_id,
            bucket_name,
            auth_token,
            events_table: events_table.unwrap_or_else(|| "default.hogflare_events".to_string()),
        }
    }
}

#[derive(Clone)]
#[cfg_attr(target_arch = "wasm32", allow(dead_code))]
pub struct ProductAnalyticsClient {
    config: ProductAnalyticsConfig,
    #[cfg(not(target_arch = "wasm32"))]
    worker: std::sync::Arc<Mutex<Option<SidemanticWorker>>>,
    #[cfg(not(target_arch = "wasm32"))]
    request_id: std::sync::Arc<AtomicU64>,
}

impl ProductAnalyticsClient {
    pub fn new(config: ProductAnalyticsConfig) -> Self {
        Self {
            config,
            #[cfg(not(target_arch = "wasm32"))]
            worker: std::sync::Arc::new(Mutex::new(None)),
            #[cfg(not(target_arch = "wasm32"))]
            request_id: std::sync::Arc::new(AtomicU64::new(0)),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn query(
        &self,
        query: ProductAnalyticsQuery,
    ) -> Result<ProductAnalyticsResponse, ProductAnalyticsError> {
        let request_id = self.request_id.fetch_add(1, Ordering::Relaxed) + 1;
        let mut payload_value =
            serde_json::to_value(query).map_err(ProductAnalyticsError::Serialize)?;
        if let Value::Object(fields) = &mut payload_value {
            fields.insert("_request_id".to_string(), json!(request_id));
        }
        let payload =
            serde_json::to_string(&payload_value).map_err(ProductAnalyticsError::Serialize)?;
        let runtime = SidemanticRuntimeConfig::from_config(&self.config);
        let mut worker = self.worker.lock().await;
        if worker
            .as_ref()
            .map(|existing| existing.runtime != runtime)
            .unwrap_or(true)
        {
            *worker = Some(SidemanticWorker::start(runtime.clone(), &self.config).await?);
        }

        let active = worker
            .as_mut()
            .expect("sidemantic worker must be initialized before querying");
        match active.query(&payload, request_id).await {
            Ok(response) => Ok(response),
            Err(err) if err.should_restart_worker() => {
                drop(worker.take());
                let mut restarted = SidemanticWorker::start(runtime, &self.config).await?;
                let response = restarted
                    .query(&payload, request_id)
                    .await
                    .map_err(SidemanticWorkerError::into_product_analytics_error)?;
                *worker = Some(restarted);
                Ok(response)
            }
            Err(err) => Err(err.into_product_analytics_error()),
        }
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn query(
        &self,
        _query: ProductAnalyticsQuery,
    ) -> Result<ProductAnalyticsResponse, ProductAnalyticsError> {
        Err(ProductAnalyticsError::Unavailable)
    }
}

#[derive(Debug, Error)]
pub enum ProductAnalyticsError {
    #[error("product analytics is not configured")]
    NotConfigured,
    #[error("failed to serialize sidemantic analytics query: {0}")]
    Serialize(#[source] serde_json::Error),
    #[error("failed to run sidemantic analytics: {0}")]
    #[cfg(not(target_arch = "wasm32"))]
    Process(#[source] std::io::Error),
    #[error("sidemantic analytics failed: {0}")]
    Failed(String),
    #[error("sidemantic analytics requires the native runtime")]
    #[cfg(target_arch = "wasm32")]
    Unavailable,
    #[error("invalid sidemantic analytics response: {0}")]
    InvalidResponse(#[source] serde_json::Error),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ProductAnalyticsQuery {
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
    #[serde(default)]
    pub metric: Option<String>,
    #[serde(default)]
    pub dimension: Option<String>,
    #[serde(default)]
    pub granularity: Option<String>,
    #[serde(default)]
    pub semantic_filters: Option<String>,
    #[serde(default)]
    pub panel: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct ProductAnalyticsResponse {
    pub focus: ProductAnalyticsFocus,
    pub summary: Vec<ProductAnalyticsMetric>,
    pub series: Vec<ProductAnalyticsSeriesPoint>,
    pub breakdowns: Vec<ProductAnalyticsBreakdown>,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct ProductAnalyticsFocus {
    pub metric: String,
    pub metric_label: String,
    pub dimension: String,
    pub dimension_label: String,
    pub granularity: String,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct ProductAnalyticsMetric {
    pub label: String,
    pub value: f64,
    pub display_value: String,
    pub model: String,
    pub metric: String,
    pub semantic_ref: String,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct ProductAnalyticsSeriesPoint {
    pub bucket: String,
    pub event_count: usize,
    pub pageviews: usize,
    pub session_count: usize,
    pub recordings: usize,
    pub focused_value: f64,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct ProductAnalyticsBreakdown {
    pub title: String,
    pub model: String,
    pub dimension: String,
    pub metric: String,
    pub rows: Vec<ProductAnalyticsBreakdownRow>,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct ProductAnalyticsBreakdownRow {
    pub label: String,
    pub value: f64,
    pub percent: f64,
    #[serde(default, skip_serializing_if = "is_false")]
    pub is_other: bool,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, PartialEq, Eq)]
struct SidemanticRuntimeConfig {
    manifest_dir: PathBuf,
    script_path: PathBuf,
    model_dir: String,
    persons_table: String,
}

#[cfg(not(target_arch = "wasm32"))]
impl SidemanticRuntimeConfig {
    fn from_config(config: &ProductAnalyticsConfig) -> Self {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let script_path = std::env::var("HOGFLARE_ANALYTICS_SIDEMANTIC_SCRIPT")
            .map(PathBuf::from)
            .unwrap_or_else(|_| manifest_dir.join("scripts/product_analytics_sidemantic.py"));
        let model_dir = std::env::var("HOGFLARE_ANALYTICS_MODEL_DIR")
            .unwrap_or_else(|_| manifest_dir.join("models").display().to_string());
        let persons_table = std::env::var("HOGFLARE_ANALYTICS_PERSONS_TABLE")
            .unwrap_or_else(|_| infer_persons_table(&config.events_table));

        Self {
            manifest_dir,
            script_path,
            model_dir,
            persons_table,
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct SidemanticWorker {
    runtime: SidemanticRuntimeConfig,
    child: Child,
    stdin: ChildStdin,
    stdout: Lines<BufReader<ChildStdout>>,
}

#[cfg(not(target_arch = "wasm32"))]
impl SidemanticWorker {
    async fn start(
        runtime: SidemanticRuntimeConfig,
        config: &ProductAnalyticsConfig,
    ) -> Result<Self, ProductAnalyticsError> {
        let mut child = Command::new("uv")
            .arg("run")
            .arg("--script")
            .arg(&runtime.script_path)
            .arg("--serve")
            .current_dir(&runtime.manifest_dir)
            .env("HOGFLARE_ANALYTICS_ACCOUNT_ID", &config.account_id)
            .env("HOGFLARE_ANALYTICS_BUCKET", &config.bucket_name)
            .env("HOGFLARE_ANALYTICS_R2_SQL_TOKEN", &config.auth_token)
            .env("HOGFLARE_ANALYTICS_EVENTS_TABLE", &config.events_table)
            .env("HOGFLARE_ANALYTICS_PERSONS_TABLE", &runtime.persons_table)
            .env("HOGFLARE_ANALYTICS_MODEL_DIR", &runtime.model_dir)
            .env("UV_NO_PROGRESS", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(ProductAnalyticsError::Process)?;

        let stdin = child.stdin.take().ok_or_else(|| {
            ProductAnalyticsError::Process(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "sidemantic worker stdin was not available",
            ))
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            ProductAnalyticsError::Process(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "sidemantic worker stdout was not available",
            ))
        })?;

        Ok(Self {
            runtime,
            child,
            stdin,
            stdout: BufReader::new(stdout).lines(),
        })
    }

    async fn query(
        &mut self,
        payload: &str,
        request_id: u64,
    ) -> Result<ProductAnalyticsResponse, SidemanticWorkerError> {
        self.stdin
            .write_all(payload.as_bytes())
            .await
            .map_err(SidemanticWorkerError::Process)?;
        self.stdin
            .write_all(b"\n")
            .await
            .map_err(SidemanticWorkerError::Process)?;
        self.stdin
            .flush()
            .await
            .map_err(SidemanticWorkerError::Process)?;

        loop {
            let Some(line) = self
                .stdout
                .next_line()
                .await
                .map_err(SidemanticWorkerError::Process)?
            else {
                return Err(SidemanticWorkerError::Eof);
            };
            if line.trim().is_empty() {
                continue;
            }
            let envelope: SidemanticWorkerEnvelope =
                serde_json::from_str(&line).map_err(SidemanticWorkerError::InvalidResponse)?;
            if envelope.request_id != Some(request_id) {
                continue;
            }
            if envelope.ok {
                return envelope.result.ok_or_else(|| {
                    SidemanticWorkerError::Analytics(
                        "sidemantic worker returned ok without a result".to_string(),
                    )
                });
            }
            return Err(SidemanticWorkerError::Analytics(
                envelope
                    .error
                    .unwrap_or_else(|| "sidemantic worker returned an analytics error".to_string()),
            ));
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for SidemanticWorker {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Deserialize)]
struct SidemanticWorkerEnvelope {
    ok: bool,
    request_id: Option<u64>,
    result: Option<ProductAnalyticsResponse>,
    error: Option<String>,
}

#[cfg(not(target_arch = "wasm32"))]
enum SidemanticWorkerError {
    Process(std::io::Error),
    Eof,
    InvalidResponse(serde_json::Error),
    Analytics(String),
}

#[cfg(not(target_arch = "wasm32"))]
impl SidemanticWorkerError {
    fn should_restart_worker(&self) -> bool {
        matches!(
            self,
            Self::Process(_) | Self::Eof | Self::InvalidResponse(_)
        )
    }

    fn into_product_analytics_error(self) -> ProductAnalyticsError {
        match self {
            Self::Process(err) => ProductAnalyticsError::Process(err),
            Self::Eof => ProductAnalyticsError::Process(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "sidemantic worker exited before returning analytics",
            )),
            Self::InvalidResponse(err) => ProductAnalyticsError::InvalidResponse(err),
            Self::Analytics(message) => ProductAnalyticsError::Failed(message),
        }
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[cfg(not(target_arch = "wasm32"))]
fn infer_persons_table(events_table: &str) -> String {
    for (needle, replacement) in [
        ("hogflare_events_v3", "hogflare_persons_v2"),
        ("hogflare_events", "hogflare_persons"),
        ("events", "persons"),
    ] {
        if events_table.contains(needle) {
            return events_table.replace(needle, replacement);
        }
    }
    "default.hogflare_persons_v2".to_string()
}
