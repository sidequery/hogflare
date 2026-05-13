#[path = "helpers/mod.rs"]
mod helpers;

use helpers::{
    cleanup, collect_events_until, spawn_app, spawn_app_with_options,
    spawn_app_with_runtime_options, spawn_pipeline_stub, wait_for_events,
};
use hogflare::feature_flags::FeatureFlagStore;
use hogflare::groups::GroupTypeMap;
use serde_json::Value;
use tokio::process::Command;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn posthog_node_capture_is_forwarded_to_pipeline() -> Result<(), Box<dyn std::error::Error>> {
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
async fn posthog_node_group_identify_and_grouped_capture_use_sdk_shapes(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pipeline_endpoint, mut pipeline_rx, pipeline_handle) = spawn_pipeline_stub().await?;
    let group_type_map = GroupTypeMap::new([Some("company".to_string()), None, None, None, None]);
    let (address, server_handle) = spawn_app_with_runtime_options(
        pipeline_endpoint,
        None,
        None,
        None,
        None,
        None,
        group_type_map,
    )
    .await?;

    let status = Command::new("bun")
        .arg("run")
        .arg("posthog_node_group_flow.js")
        .current_dir("tests/js_client")
        .env("HOGFLARE_HOST", format!("http://{}", address))
        .env("HOGFLARE_API_KEY", "phc_test_node_key")
        .env("HOGFLARE_DISTINCT_ID", "node-group-user")
        .status()
        .await?;

    if !status.success() {
        return Err(format!("posthog node group script exited with status {status:?}").into());
    }

    let events =
        collect_events_until(&mut pipeline_rx, 2, std::time::Duration::from_secs(10)).await?;
    let group_event = events
        .iter()
        .find(|event| event["event"] == "$groupidentify")
        .expect("expected node $groupidentify event in pipeline payload");
    assert_eq!(group_event["source"], "posthog");
    assert_eq!(group_event["extra"]["group_type"], "company");
    assert_eq!(group_event["extra"]["group_key"], "node-acme");
    assert_eq!(group_event["group0"], "node-acme");
    assert_eq!(
        group_event["group_properties"]["company"]["plan"],
        "enterprise"
    );
    assert_eq!(group_event["group_properties"]["company"]["seats"], 12);

    let capture_event = events
        .iter()
        .find(|event| event["event"] == "node-grouped-capture")
        .expect("expected node grouped capture event in pipeline payload");
    assert_eq!(capture_event["source"], "posthog");
    assert_eq!(capture_event["distinct_id"], "node-group-user");
    assert_eq!(capture_event["group0"], "node-acme");
    assert_eq!(
        capture_event["group_properties"]["company"]["plan"],
        "enterprise"
    );
    assert_eq!(capture_event["properties"]["client"], "posthog-node");

    cleanup(server_handle, pipeline_handle).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn posthog_node_feature_flags_are_evaluated() -> Result<(), Box<dyn std::error::Error>> {
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

    let (address, server_handle) =
        spawn_app_with_options(pipeline_endpoint, None, None, None, Some(flags)).await?;

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
async fn posthog_node_official_flag_shapes_match_sdk_remote_evaluation(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pipeline_endpoint, _pipeline_rx, pipeline_handle) = spawn_pipeline_stub().await?;
    let flags = FeatureFlagStore::from_json(
        r#"{
  "group_type_mapping": { "0": "company" },
  "flags": [
    {
      "key": "sdk-distinct-flag",
      "active": true,
      "filters": {
        "groups": [
          {
            "properties": [
              { "key": "distinct_id", "value": "node-official-flag-user", "type": "person", "operator": "exact" }
            ],
            "rollout_percentage": 100
          }
        ]
      }
    },
    {
      "key": "sdk-group-key-flag",
      "active": true,
      "filters": {
        "aggregation_group_type_index": 0,
        "groups": [
          {
            "properties": [
              { "key": "$group_key", "value": "node-flags-company", "type": "group", "operator": "exact" }
            ],
            "rollout_percentage": 100
          }
        ]
      }
    },
    {
      "key": "sdk-group-plan-flag",
      "active": true,
      "filters": {
        "aggregation_group_type_index": 0,
        "groups": [
          {
            "properties": [
              { "key": "plan", "value": "enterprise", "type": "group", "operator": "exact" }
            ],
            "rollout_percentage": 100
          }
        ]
      }
    },
    {
      "key": "sdk-variant-flag",
      "active": true,
      "filters": {
        "groups": [
          { "properties": [], "rollout_percentage": 100 }
        ],
        "multivariate": {
          "variants": [
            { "key": "control", "rollout_percentage": 0 },
            { "key": "test", "rollout_percentage": 100 }
          ]
        },
        "payloads": { "test": "{\"copy\":\"sdk\"}" }
      }
    }
  ]
}"#,
    )?;

    let group_type_map = GroupTypeMap::new([Some("company".to_string()), None, None, None, None]);
    let (address, server_handle) = spawn_app_with_runtime_options(
        pipeline_endpoint,
        None,
        None,
        None,
        None,
        Some(flags),
        group_type_map,
    )
    .await?;

    let output = Command::new("bun")
        .arg("run")
        .arg("posthog_node_official_flags.js")
        .current_dir("tests/js_client")
        .env("HOGFLARE_HOST", format!("http://{}", address))
        .env("HOGFLARE_API_KEY", "phc_test_node_key")
        .env("HOGFLARE_DISTINCT_ID", "node-official-flag-user")
        .env("HOGFLARE_GROUP_KEY", "node-flags-company")
        .output()
        .await?;

    if !output.status.success() {
        return Err(format!(
            "posthog node official flags script exited with status {:?}",
            output.status
        )
        .into());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let result: Value = serde_json::from_str(stdout.trim())?;
    assert_eq!(result["distinct"], Value::Bool(true));
    assert_eq!(result["groupKeyFlag"], Value::Bool(true));
    assert_eq!(result["groupPlan"], Value::Bool(true));
    assert_eq!(result["variant"], Value::String("test".to_string()));
    assert_eq!(result["payload"]["copy"], Value::String("sdk".to_string()));

    cleanup(server_handle, pipeline_handle).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn posthog_node_flags_update_after_capture() -> Result<(), Box<dyn std::error::Error>> {
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

    let (address, server_handle) =
        spawn_app_with_options(pipeline_endpoint, None, None, None, Some(flags)).await?;

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
