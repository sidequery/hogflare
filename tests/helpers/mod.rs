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
    spawn_app_with_options(pipeline_endpoint, None, None, None).await
}

pub async fn spawn_app_with_options(
    pipeline_endpoint: Url,
    decide_api_token: Option<String>,
    session_recording_endpoint: Option<String>,
    signing_secret: Option<String>,
) -> Result<(SocketAddr, JoinHandle<()>), Box<dyn std::error::Error>> {
    let pipeline_client = PipelineClient::new(pipeline_endpoint, None, Duration::from_secs(5))?;

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;

    let server_handle = tokio::spawn({
        let pipeline = Arc::new(pipeline_client);
        async move {
            if let Err(err) = hogflare::serve_with_options(
                listener,
                pipeline,
                decide_api_token,
                session_recording_endpoint,
                signing_secret,
                None,
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
