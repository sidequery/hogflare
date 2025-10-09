#[path = "helpers/mod.rs"]
mod helpers;

use helpers::{cleanup, spawn_app, spawn_pipeline_stub, wait_for_events};
use reqwest::Client;
use serde_json::{json, Value};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn posthog_compatibility_endpoints_forward_events() -> Result<(), Box<dyn std::error::Error>>
{
    let (pipeline_endpoint, mut pipeline_rx, pipeline_handle) = spawn_pipeline_stub().await?;
    let (address, server_handle) = spawn_app(pipeline_endpoint).await?;
    let base_url = format!("http://{}", address);
    let client = Client::builder().timeout(Duration::from_secs(5)).build()?;

    // capture
    let capture_payload = json!({
        "event": "integration-capture",
        "distinct_id": "capture-user",
        "api_key": "phc_capture",
        "properties": {"plan": "pro"},
        "library": "tests",
    });

    let capture_response = client
        .post(format!("{}/capture", base_url))
        .json(&capture_payload)
        .send()
        .await?;
    assert!(capture_response.status().is_success());

    let capture_events = wait_for_events(&mut pipeline_rx).await?;
    assert_eq!(capture_events.len(), 1);
    assert_eq!(capture_events[0]["event_type"], "integration-capture");
    assert_eq!(capture_events[0]["distinct_id"], "capture-user");
    assert_eq!(capture_events[0]["api_key"], "phc_capture");
    assert_eq!(
        capture_events[0]["extra"]["library"],
        Value::String("tests".to_string())
    );

    // identify
    let identify_payload = json!({
        "distinct_id": "identify-user",
        "api_key": "phc_identify",
        "properties": {"email": "id@example.com"},
        "context": {"ip": "127.0.0.1"},
    });

    let identify_response = client
        .post(format!("{}/identify", base_url))
        .json(&identify_payload)
        .send()
        .await?;
    assert!(identify_response.status().is_success());

    let identify_events = wait_for_events(&mut pipeline_rx).await?;
    assert_eq!(identify_events.len(), 1);
    let identify_event = &identify_events[0];
    assert_eq!(identify_event["event_type"], "$identify");
    assert_eq!(identify_event["distinct_id"], "identify-user");
    assert_eq!(identify_event["context"]["ip"], "127.0.0.1");
    assert_eq!(
        identify_event["person_properties"]["email"],
        "id@example.com"
    );

    // group identify
    let groups_payload = json!({
        "group_type": "team",
        "group_key": "team-42",
        "api_key": "phc_group",
        "properties": {"members": 5},
    });

    let groups_response = client
        .post(format!("{}/groups", base_url))
        .json(&groups_payload)
        .send()
        .await?;
    assert!(groups_response.status().is_success());

    let group_events = wait_for_events(&mut pipeline_rx).await?;
    assert_eq!(group_events.len(), 1);
    let group_event = &group_events[0];
    assert_eq!(group_event["event_type"], "$groupidentify");
    assert_eq!(group_event["distinct_id"], "team-42");
    assert_eq!(group_event["extra"]["group_type"], "team");
    assert_eq!(group_event["extra"]["group_key"], "team-42");
    assert_eq!(group_event["properties"]["members"], 5);

    // batch
    let batch_payload = json!({
        "api_key": "phc_batch",
        "batch": [
            {
                "event": "batched-one",
                "distinct_id": "batch-user-a",
                "properties": {"from": "shared"},
            },
            {
                "event": "batched-two",
                "distinct_id": "batch-user-b",
                "api_key": "phc_override",
            }
        ]
    });

    let batch_response = client
        .post(format!("{}/batch", base_url))
        .json(&batch_payload)
        .send()
        .await?;
    assert!(batch_response.status().is_success());

    let batch_events = wait_for_events(&mut pipeline_rx).await?;
    assert_eq!(batch_events.len(), 2);
    assert_eq!(batch_events[0]["event_type"], "batched-one");
    assert_eq!(batch_events[0]["api_key"], "phc_batch");
    assert_eq!(batch_events[0]["properties"]["from"], "shared");
    assert_eq!(batch_events[1]["event_type"], "batched-two");
    assert_eq!(batch_events[1]["api_key"], "phc_override");

    // decide
    let decide_response = client
        .post(format!("{}/decide", base_url))
        .json(&json!({}))
        .send()
        .await?;
    assert!(decide_response.status().is_success());
    let decide_body: Value = decide_response.json().await?;
    assert_eq!(decide_body["status"], 200);
    assert!(decide_body["supportedCompression"]
        .as_array()
        .expect("supported_compression should be array")
        .iter()
        .any(|value| value == "gzip"));

    // health
    let health_response = client.get(format!("{}/healthz", base_url)).send().await?;
    assert!(health_response.status().is_success());
    let health_body: Value = health_response.json().await?;
    assert_eq!(health_body["status"], "ok");

    cleanup(server_handle, pipeline_handle).await;
    Ok(())
}
