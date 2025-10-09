use std::{net::SocketAddr, sync::Arc, time::Duration};

use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use hogflare::pipeline::PipelineClient;
use reqwest::Url;
use serde_json::Value;
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle, time::timeout};

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
    let pipeline_client = PipelineClient::new(pipeline_endpoint, None, Duration::from_secs(5))?;

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;

    let server_handle = tokio::spawn({
        let pipeline = Arc::new(pipeline_client);
        async move {
            if let Err(err) = hogflare::serve(listener, pipeline).await {
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
