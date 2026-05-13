#![allow(dead_code)]

use std::{net::SocketAddr, sync::Arc, time::Duration};

use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use hogflare::pipeline::PipelineClient;
use reqwest::{Client, Url};
use serde_json::Value;
use tokio::{
    net::TcpListener,
    process::Command,
    sync::{mpsc, Mutex, MutexGuard},
    task::JoinHandle,
    time::{sleep, timeout},
};

static DOCKER_PIPELINE_LOCK: Mutex<()> = Mutex::const_new(());

pub async fn spawn_pipeline_stub(
) -> Result<(Url, mpsc::Receiver<Vec<Value>>, JoinHandle<()>), Box<dyn std::error::Error>> {
    let (sender, receiver) = mpsc::channel(16);

    #[derive(Clone)]
    struct StubState {
        sender: mpsc::Sender<Vec<Value>>,
    }

    async fn handle_events(
        State(state): State<StubState>,
        Json(payload): Json<Vec<Value>>,
    ) -> StatusCode {
        let _ = state.sender.send(payload).await;
        StatusCode::OK
    }

    let app = Router::new()
        .route("/", post(handle_events))
        .with_state(StubState { sender });

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let endpoint = Url::parse(&format!("http://{}/", address))?;

    let handle = tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, app.into_make_service()).await {
            eprintln!("pipeline stub terminated: {err}");
        }
    });

    Ok((endpoint, receiver, handle))
}

pub async fn spawn_app(
    pipeline_endpoint: Url,
) -> Result<(SocketAddr, JoinHandle<()>), Box<dyn std::error::Error>> {
    spawn_app_with_options(pipeline_endpoint, None, None, None, None).await
}

pub async fn spawn_app_with_options(
    pipeline_endpoint: Url,
    decide_api_token: Option<String>,
    session_recording_endpoint: Option<String>,
    signing_secret: Option<String>,
    feature_flags: Option<hogflare::feature_flags::FeatureFlagStore>,
) -> Result<(SocketAddr, JoinHandle<()>), Box<dyn std::error::Error>> {
    spawn_app_with_runtime_options(
        pipeline_endpoint,
        decide_api_token,
        session_recording_endpoint,
        signing_secret,
        None,
        feature_flags,
        hogflare::groups::GroupTypeMap::default(),
    )
    .await
}

pub async fn spawn_app_with_options_and_debug(
    pipeline_endpoint: Url,
    decide_api_token: Option<String>,
    session_recording_endpoint: Option<String>,
    signing_secret: Option<String>,
    feature_flags: Option<hogflare::feature_flags::FeatureFlagStore>,
    person_debug_token: Option<String>,
) -> Result<(SocketAddr, JoinHandle<()>), Box<dyn std::error::Error>> {
    spawn_app_with_runtime_options(
        pipeline_endpoint,
        decide_api_token,
        session_recording_endpoint,
        signing_secret,
        person_debug_token,
        feature_flags,
        hogflare::groups::GroupTypeMap::default(),
    )
    .await
}

pub async fn spawn_app_with_runtime_options(
    pipeline_endpoint: Url,
    decide_api_token: Option<String>,
    session_recording_endpoint: Option<String>,
    signing_secret: Option<String>,
    person_debug_token: Option<String>,
    feature_flags: Option<hogflare::feature_flags::FeatureFlagStore>,
    group_type_map: hogflare::groups::GroupTypeMap,
) -> Result<(SocketAddr, JoinHandle<()>), Box<dyn std::error::Error>> {
    spawn_app_with_runtime_options_and_person_pipeline(
        pipeline_endpoint,
        None,
        decide_api_token,
        session_recording_endpoint,
        signing_secret,
        person_debug_token,
        feature_flags,
        group_type_map,
    )
    .await
}

pub async fn spawn_app_with_options_debug_and_person_pipeline(
    pipeline_endpoint: Url,
    persons_pipeline_endpoint: Option<Url>,
    decide_api_token: Option<String>,
    session_recording_endpoint: Option<String>,
    signing_secret: Option<String>,
    feature_flags: Option<hogflare::feature_flags::FeatureFlagStore>,
    person_debug_token: Option<String>,
) -> Result<(SocketAddr, JoinHandle<()>), Box<dyn std::error::Error>> {
    spawn_app_with_runtime_options_and_person_pipeline(
        pipeline_endpoint,
        persons_pipeline_endpoint,
        decide_api_token,
        session_recording_endpoint,
        signing_secret,
        person_debug_token,
        feature_flags,
        hogflare::groups::GroupTypeMap::default(),
    )
    .await
}

pub async fn spawn_app_with_runtime_options_and_person_pipeline(
    pipeline_endpoint: Url,
    persons_pipeline_endpoint: Option<Url>,
    decide_api_token: Option<String>,
    session_recording_endpoint: Option<String>,
    signing_secret: Option<String>,
    person_debug_token: Option<String>,
    feature_flags: Option<hogflare::feature_flags::FeatureFlagStore>,
    group_type_map: hogflare::groups::GroupTypeMap,
) -> Result<(SocketAddr, JoinHandle<()>), Box<dyn std::error::Error>> {
    let pipeline_client = PipelineClient::new(pipeline_endpoint, None, Duration::from_secs(5))?;
    let persons_pipeline_client = persons_pipeline_endpoint
        .map(|endpoint| PipelineClient::new(endpoint, None, Duration::from_secs(5)).map(Arc::new))
        .transpose()?;

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;

    let server_handle = tokio::spawn({
        let pipeline = Arc::new(pipeline_client);
        async move {
            if let Err(err) = hogflare::serve_with_person_pipeline(
                listener,
                pipeline,
                persons_pipeline_client,
                None,
                Arc::new(hogflare::groups::MemoryGroupStore::new()),
                group_type_map,
                decide_api_token,
                session_recording_endpoint,
                signing_secret,
                person_debug_token,
                Arc::new(
                    feature_flags.unwrap_or_else(hogflare::feature_flags::FeatureFlagStore::empty),
                ),
            )
            .await
            {
                eprintln!("hogflare server terminated: {err}");
            }
        }
    });

    Ok((address, server_handle))
}

pub async fn wait_for_events(
    receiver: &mut mpsc::Receiver<Vec<Value>>,
) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
    match timeout(Duration::from_secs(10), receiver.recv()).await {
        Ok(Some(events)) => Ok(events),
        Ok(None) => Err("pipeline receiver closed unexpectedly".into()),
        Err(_) => Err("timed out waiting for pipeline payload".into()),
    }
}

pub async fn collect_events_until(
    receiver: &mut mpsc::Receiver<Vec<Value>>,
    min_count: usize,
    deadline_after: Duration,
) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
    let deadline = tokio::time::Instant::now() + deadline_after;
    let mut all_events = Vec::new();

    while all_events.len() < min_count && tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }

        match timeout(remaining.min(Duration::from_millis(500)), receiver.recv()).await {
            Ok(Some(events)) => all_events.extend(events),
            Ok(None) => return Err("pipeline receiver closed unexpectedly".into()),
            Err(_) => {}
        }
    }

    if all_events.len() < min_count {
        return Err(format!(
            "timed out waiting for {min_count} events, received {}",
            all_events.len()
        )
        .into());
    }

    Ok(all_events)
}

pub async fn cleanup(server_handle: JoinHandle<()>, pipeline_handle: JoinHandle<()>) {
    server_handle.abort();
    let _ = server_handle.await;
    pipeline_handle.abort();
    let _ = pipeline_handle.await;
}

pub async fn wait_for_pipeline_events(
    client: &Client,
    url: &Url,
    min_count: usize,
) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
    for _ in 0..100 {
        if let Ok(response) = client.get(url.clone()).send().await {
            if response.status().is_success() {
                let events: Vec<Value> = response.json().await?;
                if events.len() >= min_count {
                    return Ok(events);
                }
            }
        }
        sleep(Duration::from_millis(200)).await;
    }

    Err("timed out waiting for pipeline events".into())
}

pub async fn start_docker_pipeline(
) -> Result<(Url, MutexGuard<'static, ()>), Box<dyn std::error::Error>> {
    let guard = DOCKER_PIPELINE_LOCK.lock().await;

    let status = Command::new("docker")
        .arg("compose")
        .arg("up")
        .arg("--build")
        .arg("--force-recreate")
        .arg("-d")
        .arg("fake-pipeline")
        .status()
        .await?;

    if !status.success() {
        return Err("failed to start docker compose pipeline".into());
    }

    let client = Client::builder().timeout(Duration::from_secs(2)).build()?;
    let base_url = Url::parse("http://127.0.0.1:8088/")?;
    let health_url = base_url.join("health")?;

    for _ in 0..100 {
        if let Ok(response) = client.get(health_url.clone()).send().await {
            if response.status().is_success() {
                let reset_url = base_url.join("reset")?;
                let _ = client.post(reset_url).send().await;
                return Ok((base_url, guard));
            }
        }
        sleep(Duration::from_millis(200)).await;
    }

    Err("docker pipeline failed to report healthy state".into())
}

pub async fn stop_docker_pipeline() -> Result<(), Box<dyn std::error::Error>> {
    let status = Command::new("docker")
        .arg("compose")
        .arg("down")
        .arg("--remove-orphans")
        .status()
        .await?;

    if !status.success() {
        return Err("failed to stop docker compose pipeline".into());
    }

    Ok(())
}
