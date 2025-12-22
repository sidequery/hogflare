#[path = "helpers/mod.rs"]
mod helpers;

use helpers::{cleanup, spawn_app, spawn_app_with_options, spawn_pipeline_stub, wait_for_events};
use hogflare::feature_flags::FeatureFlagStore;
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
        .find(|event| event["event"] == "node-integration-test")
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn posthog_node_feature_flags_are_evaluated(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pipeline_endpoint, _pipeline_rx, pipeline_handle) = spawn_pipeline_stub().await?;
    let flags = FeatureFlagStore::from_json(
        r#"{
  "flags": [
    {
      "key": "pro-flag",
      "type": "boolean",
      "active": true,
      "rollout_percentage": 100,
      "conditions": [
        {
          "properties": [
            { "key": "plan", "value": "pro", "type": "person" }
          ]
        }
      ],
      "payload": { "tier": "pro" }
    }
  ]
}"#,
    )?;

    let (address, server_handle) = spawn_app_with_options(
        pipeline_endpoint,
        None,
        None,
        None,
        Some(flags),
    )
    .await?;

    let output = Command::new("bun")
        .arg("run")
        .arg("posthog_node_flags.js")
        .current_dir("tests/js_client")
        .env("HOGFLARE_HOST", format!("http://{}", address))
        .env("HOGFLARE_API_KEY", "phc_test_node_key")
        .env("HOGFLARE_DISTINCT_ID", "node-flag-user")
        .output()
        .await?;

    if !output.status.success() {
        return Err(format!(
            "posthog node flags script exited with status {:?}",
            output.status
        )
        .into());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let result: Value = serde_json::from_str(stdout.trim())?;
    assert_eq!(result["value"], Value::Bool(true));
    assert_eq!(result["payload"]["tier"], Value::String("pro".to_string()));

    cleanup(server_handle, pipeline_handle).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn posthog_node_flags_update_after_capture(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pipeline_endpoint, _pipeline_rx, pipeline_handle) = spawn_pipeline_stub().await?;
    let flags = FeatureFlagStore::from_json(
        r#"{
  "flags": [
    {
      "key": "eligible-flag",
      "type": "boolean",
      "active": true,
      "rollout_percentage": 100,
      "conditions": [
        {
          "properties": [
            { "key": "plan", "value": "pro", "type": "person" }
          ]
        }
      ],
      "payload": { "tier": "pro" }
    }
  ]
}"#,
    )?;

    let (address, server_handle) = spawn_app_with_options(
        pipeline_endpoint,
        None,
        None,
        None,
        Some(flags),
    )
    .await?;

    let output = Command::new("bun")
        .arg("run")
        .arg("posthog_node_flag_eligibility.js")
        .current_dir("tests/js_client")
        .env("HOGFLARE_HOST", format!("http://{}", address))
        .env("HOGFLARE_API_KEY", "phc_test_node_key")
        .env("HOGFLARE_DISTINCT_ID", "node-eligibility-user")
        .output()
        .await?;

    if !output.status.success() {
        return Err(format!(
            "posthog node eligibility script exited with status {:?}",
            output.status
        )
        .into());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let result: Value = serde_json::from_str(stdout.trim())?;
    assert_eq!(result["before"], Value::Bool(false));
    assert_eq!(result["after"], Value::Bool(true));
    assert_eq!(result["payload"]["tier"], Value::String("pro".to_string()));

    cleanup(server_handle, pipeline_handle).await;
    Ok(())
}
