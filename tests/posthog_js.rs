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
    assert_eq!(event["event"], "js-integration-test");
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
            spawn_app_with_options(pipeline_base.clone(), None, None, None, None).await?;

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
                .find(|event| event["event"] == "js-integration-test")
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn posthog_js_identify_is_forwarded_to_pipeline() -> Result<(), Box<dyn std::error::Error>> {
    let (pipeline_endpoint, mut pipeline_rx, pipeline_handle) = spawn_pipeline_stub().await?;
    let (address, server_handle) = spawn_app(pipeline_endpoint).await?;

    let status = Command::new("bun")
        .arg("run")
        .arg("posthog_identify.js")
        .current_dir("tests/js_client")
        .env("HOGFLARE_HOST", format!("http://{}", address))
        .env("HOGFLARE_API_KEY", "phc_test_integration_key")
        .env("HOGFLARE_DISTINCT_ID", "js-integration-user")
        .status()
        .await?;

    if !status.success() {
        return Err(format!("posthog identify script exited with status {status:?}").into());
    }

    let events = wait_for_events(&mut pipeline_rx).await?;
    let event = events
        .iter()
        .find(|e| e["event"] == "$identify")
        .expect("expected $identify event in pipeline payload");

    assert_eq!(event["source"], "posthog");
    assert_eq!(event["distinct_id"], "identified-user-123");

    let person_props = event
        .get("person_properties")
        .and_then(Value::as_object)
        .expect("identify should include person_properties");
    assert_eq!(
        person_props.get("email").and_then(Value::as_str),
        Some("test@example.com")
    );
    assert_eq!(
        person_props.get("plan").and_then(Value::as_str),
        Some("enterprise")
    );

    cleanup(server_handle, pipeline_handle).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn posthog_js_group_is_forwarded_to_pipeline() -> Result<(), Box<dyn std::error::Error>> {
    let (pipeline_endpoint, mut pipeline_rx, pipeline_handle) = spawn_pipeline_stub().await?;
    let (address, server_handle) = spawn_app(pipeline_endpoint).await?;

    let status = Command::new("bun")
        .arg("run")
        .arg("posthog_group.js")
        .current_dir("tests/js_client")
        .env("HOGFLARE_HOST", format!("http://{}", address))
        .env("HOGFLARE_API_KEY", "phc_test_integration_key")
        .env("HOGFLARE_DISTINCT_ID", "js-integration-user")
        .status()
        .await?;

    if !status.success() {
        return Err(format!("posthog group script exited with status {status:?}").into());
    }

    let events = wait_for_events(&mut pipeline_rx).await?;
    let event = events
        .iter()
        .find(|e| e["event"] == "$groupidentify")
        .expect("expected $groupidentify event in pipeline payload");

    assert_eq!(event["source"], "posthog");
    assert_eq!(event["extra"]["group_type"], "company");
    assert_eq!(event["extra"]["group_key"], "acme-corp");

    cleanup(server_handle, pipeline_handle).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn posthog_js_multiple_events_forwarded_to_pipeline() -> Result<(), Box<dyn std::error::Error>>
{
    let (pipeline_endpoint, mut pipeline_rx, pipeline_handle) = spawn_pipeline_stub().await?;
    let (address, server_handle) = spawn_app(pipeline_endpoint).await?;

    let status = Command::new("bun")
        .arg("run")
        .arg("posthog_multi.js")
        .current_dir("tests/js_client")
        .env("HOGFLARE_HOST", format!("http://{}", address))
        .env("HOGFLARE_API_KEY", "phc_test_integration_key")
        .env("HOGFLARE_DISTINCT_ID", "js-integration-user")
        .status()
        .await?;

    if !status.success() {
        return Err(format!("posthog multi script exited with status {status:?}").into());
    }

    // Collect events (may arrive in multiple batches)
    let mut all_events: Vec<Value> = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);

    while all_events.len() < 4 && tokio::time::Instant::now() < deadline {
        if let Ok(events) = tokio::time::timeout(
            Duration::from_millis(500),
            wait_for_events(&mut pipeline_rx),
        )
        .await
        {
            if let Ok(batch) = events {
                all_events.extend(batch);
            }
        }
    }

    let event_types: Vec<&str> = all_events
        .iter()
        .filter_map(|e| e["event"].as_str())
        .collect();

    assert!(
        event_types.contains(&"page_view"),
        "expected page_view event, got: {:?}",
        event_types
    );
    assert!(
        event_types.contains(&"button_click"),
        "expected button_click event, got: {:?}",
        event_types
    );
    assert!(
        event_types.contains(&"form_submit"),
        "expected form_submit event, got: {:?}",
        event_types
    );
    assert!(
        event_types.contains(&"signup_complete"),
        "expected signup_complete event, got: {:?}",
        event_types
    );

    cleanup(server_handle, pipeline_handle).await;
    Ok(())
}
