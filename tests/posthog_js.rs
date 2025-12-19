#[path = "helpers/mod.rs"]
mod helpers;

use std::time::Duration;

use helpers::{
    cleanup, spawn_app, spawn_app_with_options, spawn_pipeline_stub, start_docker_pipeline,
    stop_docker_pipeline, wait_for_events, wait_for_pipeline_events,
};
use reqwest::Client;
use serde_json::Value;
use tokio::process::Command;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn posthog_js_capture_is_forwarded_to_pipeline() -> Result<(), Box<dyn std::error::Error>> {
    let (pipeline_endpoint, mut pipeline_rx, pipeline_handle) = spawn_pipeline_stub().await?;
    let (address, server_handle) = spawn_app(pipeline_endpoint).await?;

    let status = Command::new("bun")
        .arg("run")
        .arg("posthog_capture.js")
        .current_dir("tests/js_client")
        .env("HOGFLARE_HOST", format!("http://{}", address))
        .env("HOGFLARE_API_KEY", "phc_test_integration_key")
        .env("HOGFLARE_DISTINCT_ID", "js-integration-user")
        .status()
        .await?;

    if !status.success() {
        return Err(format!("posthog js script exited with status {status:?}").into());
    }

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn posthog_js_pipeline_persists_events() -> Result<(), Box<dyn std::error::Error>> {
    let (pipeline_base, _pipeline_guard) = start_docker_pipeline().await?;

    let test_result = async {
        let (address, server_handle) =
            spawn_app_with_options(pipeline_base.clone(), None, None, None).await?;

        let capture_result = async {
            let status = Command::new("bun")
                .arg("run")
                .arg("posthog_capture.js")
                .current_dir("tests/js_client")
                .env("HOGFLARE_HOST", format!("http://{}", address))
                .env("HOGFLARE_API_KEY", "phc_test_integration_key")
                .env("HOGFLARE_DISTINCT_ID", "js-integration-user")
                .status()
                .await?;

            if !status.success() {
                return Err(format!("posthog js script exited with status {status:?}").into());
            }

            let client = Client::builder().timeout(Duration::from_secs(2)).build()?;
            let events_url = pipeline_base.join("events")?;
            let events = wait_for_pipeline_events(&client, &events_url, 1).await?;

            let event = events
                .iter()
                .find(|event| event["event_type"] == "js-integration-test")
                .expect("expected js-integration-test event in pipeline");

            assert_eq!(event["distinct_id"], "js-integration-user");
            assert_eq!(event["source"], "posthog");
            assert_eq!(
                event["properties"]["framework"].as_str(),
                Some("integration"),
            );

            Ok(()) as Result<(), Box<dyn std::error::Error>>
        }
        .await;

        server_handle.abort();
        let _ = server_handle.await;
        capture_result
    }
    .await;

    stop_docker_pipeline().await.ok();
    test_result
}
