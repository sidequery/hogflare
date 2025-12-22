#[path = "helpers/mod.rs"]
mod helpers;

use helpers::{cleanup, spawn_app, spawn_pipeline_stub, wait_for_events};
use serde_json::Value;
use tokio::process::Command;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn posthog_node_capture_is_forwarded_to_pipeline() -> Result<(), Box<dyn std::error::Error>>
{
    let (pipeline_endpoint, mut pipeline_rx, pipeline_handle) = spawn_pipeline_stub().await?;
    let (address, server_handle) = spawn_app(pipeline_endpoint).await?;

    let status = Command::new("bun")
        .arg("run")
        .arg("posthog_node_capture.js")
        .current_dir("tests/js_client")
        .env("HOGFLARE_HOST", format!("http://{}", address))
        .env("HOGFLARE_API_KEY", "phc_test_node_key")
        .env("HOGFLARE_DISTINCT_ID", "node-integration-user")
        .status()
        .await?;

    if !status.success() {
        return Err(format!("posthog node script exited with status {status:?}").into());
    }

    let events = wait_for_events(&mut pipeline_rx).await?;
    let event = events
        .iter()
        .find(|event| event["event_type"] == "node-integration-test")
        .expect("expected node-integration-test event in pipeline payload");

    assert_eq!(event["source"], "posthog");
    assert_eq!(event["distinct_id"], "node-integration-user");

    let properties = event
        .get("properties")
        .and_then(Value::as_object)
        .expect("event payload should include properties");
    assert_eq!(
        properties.get("client").and_then(Value::as_str),
        Some("posthog-node")
    );

    cleanup(server_handle, pipeline_handle).await;
    Ok(())
}
