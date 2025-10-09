#[path = "helpers/mod.rs"]
mod helpers;

use std::path::Path;

use helpers::{cleanup, spawn_app, spawn_pipeline_stub, wait_for_events};
use serde_json::Value;
use tokio::process::Command;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn posthog_js_capture_is_forwarded_to_pipeline() -> Result<(), Box<dyn std::error::Error>> {
    let (pipeline_endpoint, mut pipeline_rx, pipeline_handle) = spawn_pipeline_stub().await?;
    let (address, server_handle) = spawn_app(pipeline_endpoint).await?;

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

    let events = wait_for_events(&mut pipeline_rx).await?;
    let event = events
        .first()
        .expect("expected at least one event in pipeline payload");

    assert_eq!(event["source"], "posthog");
    assert_eq!(event["event_type"], "js-integration-test");
    assert_eq!(event["distinct_id"], "js-integration-user");

    let properties = event
        .get("properties")
        .and_then(Value::as_object)
        .expect("event payload should include properties");
    assert_eq!(
        properties.get("framework").and_then(Value::as_str),
        Some("integration")
    );
    assert_eq!(
        properties.get("client").and_then(Value::as_str),
        Some("posthog-js")
    );

    cleanup(server_handle, pipeline_handle).await;
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
