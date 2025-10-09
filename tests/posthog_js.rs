use std::{path::Path, sync::Arc, time::Duration};

use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use hogflare::pipeline::PipelineClient;
use reqwest::Url;
use serde_json::Value;
use tokio::{net::TcpListener, process::Command, sync::mpsc, task::JoinHandle, time::timeout};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn posthog_js_capture_is_forwarded_to_pipeline() -> Result<(), Box<dyn std::error::Error>> {
    let (pipeline_endpoint, mut pipeline_rx, pipeline_handle) = spawn_pipeline_stub().await?;

    let pipeline_client = PipelineClient::new(pipeline_endpoint, None, Duration::from_secs(5))?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;

    let server_handle = tokio::spawn({
        let pipeline = Arc::new(pipeline_client);
        async move { hogflare::serve(listener, pipeline).await }
    });

    ensure_js_dependencies_installed().await?;

    let status = Command::new("bun")
        .arg("run")
        .arg("posthog_capture.js")
        .current_dir("tests/js_client")
        .env("HOGFLARE_HOST", format!("http://{}", address))
        .env("HOGFLARE_API_KEY", "phc_test_integration_key")
        .env("HOGFLARE_DISTINCT_ID", "js-integration-user")
        .status()
        .await?;

    assert!(
        status.success(),
        "posthog js script exited with status {status:?}"
    );

    let events = timeout(Duration::from_secs(10), pipeline_rx.recv())
        .await
        .map_err(|_| "timed out waiting for pipeline payload")?
        .ok_or("pipeline receiver closed unexpectedly")?;

    let event = events
        .first()
        .expect("expected at least one event in pipeline payload");

    assert_eq!(event["source"], "posthog");
    assert_eq!(event["event_type"], "js-integration-test");
    assert_eq!(event["distinct_id"], "js-integration-user");

    let properties = event
        .get("properties")
        .and_then(|value| value.as_object())
        .expect("event payload should include properties");
    assert_eq!(
        properties.get("framework").and_then(Value::as_str),
        Some("integration"),
    );
    assert_eq!(
        properties.get("client").and_then(Value::as_str),
        Some("posthog-js"),
    );

    // Shut down servers to avoid task leaks.
    server_handle.abort();
    let _ = server_handle.await;
    pipeline_handle.abort();
    let _ = pipeline_handle.await;

    Ok(())
}

async fn ensure_js_dependencies_installed() -> Result<(), Box<dyn std::error::Error>> {
    let node_modules = Path::new("tests/js_client/node_modules");
    if node_modules.exists() {
        return Ok(());
    }

    let status = Command::new("bun")
        .arg("install")
        .current_dir("tests/js_client")
        .status()
        .await?;

    if !status.success() {
        return Err(format!("bun install failed with status {status:?}").into());
    }

    Ok(())
}

async fn spawn_pipeline_stub(
) -> Result<(Url, mpsc::Receiver<Vec<Value>>, JoinHandle<()>), Box<dyn std::error::Error>> {
    let (sender, receiver) = mpsc::channel(8);

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
