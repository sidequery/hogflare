#[path = "helpers/mod.rs"]
mod helpers;

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine};
use chrono::Utc;
use flate2::{write::GzEncoder, Compression};
use helpers::{
    cleanup, spawn_app_with_options, spawn_app_with_options_and_debug,
    spawn_app_with_options_debug_and_person_pipeline, spawn_app_with_replay,
    spawn_app_with_runtime_options, spawn_pipeline_stub, spawn_replay_sql_stub, wait_for_events,
};
use hmac::{Hmac, Mac};
use hogflare::{feature_flags::FeatureFlagStore, groups::GroupTypeMap, replay::ReplayConfig};
use reqwest::{Client, StatusCode};
use serde_json::{json, Value};
use sha2::Sha256;
use std::{io::Write, time::Duration};

fn posthog_sha256_signature(secret: &str, body: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(body.as_bytes());
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capture_requires_signature_when_secret_configured(
) -> Result<(), Box<dyn std::error::Error>> {
    let signing_secret = "test-signing-secret";
    let (pipeline_endpoint, mut pipeline_rx, pipeline_handle) = spawn_pipeline_stub().await?;
    let (address, server_handle) = spawn_app_with_options(
        pipeline_endpoint,
        None,
        None,
        Some(signing_secret.to_string()),
        None,
    )
    .await?;

    let base_url = format!("http://{}", address);
    let client = Client::builder().timeout(Duration::from_secs(5)).build()?;
    let payload = json!({
        "event": "signed-event",
        "distinct_id": "signed-user"
    });
    let body = payload.to_string();

    let unsigned = client
        .post(format!("{}/capture", base_url))
        .header("Content-Type", "application/json")
        .body(body.clone())
        .send()
        .await?;
    assert_eq!(unsigned.status(), StatusCode::UNAUTHORIZED);

    let signed = client
        .post(format!("{}/capture", base_url))
        .header("Content-Type", "application/json")
        .header("X-POSTHOG-API-KEY", "phc_signature")
        .header(
            "X-POSTHOG-SIGNATURE",
            posthog_sha256_signature(signing_secret, &body),
        )
        .body(body)
        .send()
        .await?;
    assert!(signed.status().is_success());

    let events = wait_for_events(&mut pipeline_rx).await?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["event"], "signed-event");
    assert_eq!(events[0]["api_key"], "phc_signature");

    cleanup(server_handle, pipeline_handle).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn browser_endpoint_uses_shared_signed_payload_handling(
) -> Result<(), Box<dyn std::error::Error>> {
    let signing_secret = "browser-signing-secret";
    let (pipeline_endpoint, mut pipeline_rx, pipeline_handle) = spawn_pipeline_stub().await?;
    let (address, server_handle) = spawn_app_with_options(
        pipeline_endpoint,
        None,
        None,
        Some(signing_secret.to_string()),
        None,
    )
    .await?;

    let base_url = format!("http://{}", address);
    let client = Client::builder().timeout(Duration::from_secs(5)).build()?;
    let embedded = json!({
        "event": "browser-data-envelope",
        "properties": {
            "$distinct_id": "browser-data-user",
            "source": "data-envelope"
        }
    });
    let body = json!({
        "api_key": "phc_browser_data",
        "data": BASE64_STANDARD.encode(embedded.to_string())
    })
    .to_string();

    let unsigned = client
        .post(format!("{}/e", base_url))
        .header("Content-Type", "application/json")
        .body(body.clone())
        .send()
        .await?;
    assert_eq!(unsigned.status(), StatusCode::UNAUTHORIZED);

    let signed = client
        .post(format!("{}/e", base_url))
        .header("Content-Type", "application/json")
        .header(
            "X-POSTHOG-SIGNATURE",
            posthog_sha256_signature(signing_secret, &body),
        )
        .body(body)
        .send()
        .await?;
    assert!(signed.status().is_success());

    let events = wait_for_events(&mut pipeline_rx).await?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["event"], "browser-data-envelope");
    assert_eq!(events[0]["distinct_id"], "browser-data-user");
    assert_eq!(events[0]["api_key"], "phc_browser_data");
    assert_eq!(events[0]["properties"]["source"], "data-envelope");

    cleanup(server_handle, pipeline_handle).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn posthog_error_tracking_endpoint_accepts_exception_events(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pipeline_endpoint, mut pipeline_rx, pipeline_handle) = spawn_pipeline_stub().await?;
    let (address, server_handle) =
        spawn_app_with_options(pipeline_endpoint, None, None, None, None).await?;

    let base_url = format!("http://{}", address);
    let client = Client::builder().timeout(Duration::from_secs(5)).build()?;
    let payload = json!({
        "token": "phc_exception_token",
        "event": "$exception",
        "context": {
            "session_id": "exception-context-session"
        },
        "properties": {
            "distinct_id": "manual-exception-user",
            "$exception_level": "error",
            "$exception_fingerprint": "checkout-typeerror",
            "$exception_list": [{
                "type": "TypeError",
                "value": "Cannot read properties of undefined",
                "mechanism": { "handled": true, "synthetic": false },
                "stacktrace": {
                    "type": "raw",
                    "frames": [{
                        "platform": "javascript",
                        "lang": "javascript",
                        "function": "submitCheckout",
                        "filename": "https://example.test/app.js",
                        "lineno": 42,
                        "colno": 7,
                        "in_app": true
                    }]
                }
            }]
        }
    });

    let response = client
        .post(format!("{}/i/v0/e/", base_url))
        .json(&payload)
        .send()
        .await?;
    assert!(response.status().is_success());

    let events = wait_for_events(&mut pipeline_rx).await?;
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event["source"], "posthog");
    assert_eq!(event["event"], "$exception");
    assert_eq!(event["distinct_id"], "manual-exception-user");
    assert_eq!(event["api_key"], "phc_exception_token");
    assert_eq!(event["context"]["session_id"], "exception-context-session");
    assert_eq!(
        event["properties"]["$exception_fingerprint"],
        "checkout-typeerror"
    );
    assert_eq!(
        event["properties"]["$exception_list"][0]["stacktrace"]["frames"][0]["function"],
        "submitCheckout"
    );

    cleanup(server_handle, pipeline_handle).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn error_issue_status_updates_are_written_to_pipeline(
) -> Result<(), Box<dyn std::error::Error>> {
    let admin_token = "error-tracking-admin-token";
    let (pipeline_endpoint, mut pipeline_rx, pipeline_handle) = spawn_pipeline_stub().await?;
    let (address, server_handle) = spawn_app_with_options_and_debug(
        pipeline_endpoint,
        None,
        None,
        None,
        None,
        Some(admin_token.to_string()),
    )
    .await?;

    let base_url = format!("http://{}", address);
    let client = Client::builder().timeout(Duration::from_secs(5)).build()?;

    let unauthenticated_response = client
        .patch(format!(
            "{}/errors/api/issues/{}/status",
            base_url, "checkout-typeerror"
        ))
        .json(&json!({
            "status": "resolved",
            "api_key": "phc_status_token"
        }))
        .send()
        .await?;
    assert_eq!(unauthenticated_response.status(), StatusCode::UNAUTHORIZED);

    let response = client
        .patch(format!(
            "{}/errors/api/issues/{}/status",
            base_url, "checkout-typeerror"
        ))
        .header("x-hogflare-debug-token", admin_token)
        .json(&json!({
            "status": "resolved",
            "actor": "ops@example.test",
            "reason": "fixed in deploy 2026.07.04",
            "api_key": "phc_status_token"
        }))
        .send()
        .await?;
    assert!(response.status().is_success());

    let events = wait_for_events(&mut pipeline_rx).await?;
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event["source"], "hogflare");
    assert_eq!(event["event"], "$error_issue_status");
    assert_eq!(
        event["distinct_id"],
        "hogflare:error_issue:checkout-typeerror"
    );
    assert_eq!(event["api_key"], "phc_status_token");
    assert_eq!(event["properties"]["fingerprint"], "checkout-typeerror");
    assert_eq!(event["properties"]["status"], "resolved");
    assert_eq!(event["properties"]["actor"], "ops@example.test");
    assert_eq!(event["properties"]["reason"], "fixed in deploy 2026.07.04");

    let spoofed_response = client
        .post(format!("{}/e", base_url))
        .json(&json!({
            "token": "phc_status_token",
            "event": "$error_issue_status",
            "distinct_id": "attacker",
            "properties": {
                "fingerprint": "checkout-typeerror",
                "status": "ignored"
            }
        }))
        .send()
        .await?;
    assert!(spoofed_response.status().is_success());
    let spoofed_events = wait_for_events(&mut pipeline_rx).await?;
    assert_eq!(spoofed_events[0]["source"], "posthog");
    assert_eq!(spoofed_events[0]["event"], "$error_issue_status");

    cleanup(server_handle, pipeline_handle).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn browser_identify_links_anon_distinct_id() -> Result<(), Box<dyn std::error::Error>> {
    let debug_token = "debug-browser-identify";
    let (pipeline_endpoint, mut pipeline_rx, pipeline_handle) = spawn_pipeline_stub().await?;
    let (address, server_handle) = spawn_app_with_runtime_options(
        pipeline_endpoint,
        None,
        None,
        None,
        Some(debug_token.to_string()),
        None,
        GroupTypeMap::default(),
    )
    .await?;

    let base_url = format!("http://{}", address);
    let client = Client::builder().timeout(Duration::from_secs(5)).build()?;
    let response = client
        .post(format!("{}/e", base_url))
        .json(&json!({
            "token": "phc_browser_identify",
            "event": "$identify",
            "distinct_id": "identified-browser-user",
            "properties": {
                "$anon_distinct_id": "anonymous-browser-user",
                "$set": { "email": "browser@example.com" }
            }
        }))
        .send()
        .await?;
    assert!(response.status().is_success());

    let events = wait_for_events(&mut pipeline_rx).await?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["event"], "$identify");
    assert_eq!(events[0]["distinct_id"], "identified-browser-user");
    assert_eq!(
        events[0]["person_properties"]["email"],
        "browser@example.com"
    );

    let snapshot: Value = client
        .get(format!(
            "{}/__debug/person/{}",
            base_url, "anonymous-browser-user"
        ))
        .header("x-hogflare-debug-token", debug_token)
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(snapshot["canonical_id"], "identified-browser-user");
    assert_eq!(
        snapshot["record"]["properties"]["email"],
        "browser@example.com"
    );

    cleanup(server_handle, pipeline_handle).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn groupidentify_shapes_create_and_hydrate_groups() -> Result<(), Box<dyn std::error::Error>>
{
    let (pipeline_endpoint, mut pipeline_rx, pipeline_handle) = spawn_pipeline_stub().await?;
    let group_type_map = GroupTypeMap::new([Some("team".to_string()), None, None, None, None]);
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

    let base_url = format!("http://{}", address);
    let client = Client::builder().timeout(Duration::from_secs(5)).build()?;

    let groups_response = client
        .post(format!("{}/groups", base_url))
        .json(&json!({
            "api_key": "phc_groups",
            "event": "$groupidentify",
            "distinct_id": "groups-setup",
            "properties": {
                "$group_type": "team",
                "$group_key": "team-42",
                "$group_set": { "members": 5 }
            }
        }))
        .send()
        .await?;
    assert!(groups_response.status().is_success());

    let group_events = wait_for_events(&mut pipeline_rx).await?;
    assert_eq!(group_events.len(), 1);
    assert_eq!(group_events[0]["event"], "$groupidentify");
    assert_eq!(group_events[0]["group0"], "team-42");
    assert_eq!(group_events[0]["group_properties"]["team"]["members"], 5);

    let capture_response = client
        .post(format!("{}/capture", base_url))
        .json(&json!({
            "api_key": "phc_group_capture",
            "event": "uses-created-group",
            "distinct_id": "grouped-user",
            "properties": {
                "$groups": { "team": "team-42" }
            }
        }))
        .send()
        .await?;
    assert!(capture_response.status().is_success());

    let capture_events = wait_for_events(&mut pipeline_rx).await?;
    assert_eq!(capture_events.len(), 1);
    assert_eq!(capture_events[0]["event"], "uses-created-group");
    assert_eq!(capture_events[0]["group0"], "team-42");
    assert_eq!(capture_events[0]["group_properties"]["team"]["members"], 5);

    let capture_group_response = client
        .post(format!("{}/capture", base_url))
        .json(&json!({
            "api_key": "phc_capture_groupidentify",
            "event": "$groupidentify",
            "distinct_id": "groups-setup",
            "properties": {
                "$group_type": "team",
                "$group_key": "team-43",
                "$group_set": { "members": 7 }
            }
        }))
        .send()
        .await?;
    assert!(capture_group_response.status().is_success());

    let capture_group_events = wait_for_events(&mut pipeline_rx).await?;
    assert_eq!(capture_group_events.len(), 1);
    assert_eq!(capture_group_events[0]["event"], "$groupidentify");
    assert_eq!(capture_group_events[0]["group0"], "team-43");
    assert_eq!(
        capture_group_events[0]["group_properties"]["team"]["members"],
        7
    );

    let batch_response = client
        .post(format!("{}/batch", base_url))
        .json(&json!({
            "api_key": "phc_batch_groups",
            "batch": [
                {
                    "event": "batch-distinct-in-properties",
                    "properties": {
                        "distinct_id": "batch-properties-user",
                        "$groups": { "team": "team-43" }
                    }
                },
                {
                    "event": "$groupidentify",
                    "distinct_id": "groups-setup",
                    "properties": {
                        "$group_type": "team",
                        "$group_key": "team-44",
                        "$group_set": { "members": 9 }
                    }
                }
            ]
        }))
        .send()
        .await?;
    assert!(batch_response.status().is_success());

    let batch_events = wait_for_events(&mut pipeline_rx).await?;
    assert_eq!(batch_events.len(), 2);
    assert_eq!(batch_events[0]["event"], "batch-distinct-in-properties");
    assert_eq!(batch_events[0]["distinct_id"], "batch-properties-user");
    assert_eq!(batch_events[0]["group0"], "team-43");
    assert_eq!(batch_events[0]["group_properties"]["team"]["members"], 7);
    assert_eq!(batch_events[1]["event"], "$groupidentify");
    assert_eq!(batch_events[1]["group0"], "team-44");
    assert_eq!(batch_events[1]["group_properties"]["team"]["members"], 9);

    let browser_group_response = client
        .post(format!("{}/e", base_url))
        .json(&json!({
            "token": "phc_browser_group",
            "event": "$groupidentify",
            "properties": {
                "$distinct_id": "browser-group-user",
                "$group_type": "team",
                "$group_key": "team-45",
                "$group_set": { "members": 11 }
            }
        }))
        .send()
        .await?;
    assert!(browser_group_response.status().is_success());

    let browser_group_events = wait_for_events(&mut pipeline_rx).await?;
    assert_eq!(browser_group_events.len(), 1);
    assert_eq!(browser_group_events[0]["event"], "$groupidentify");
    assert_eq!(browser_group_events[0]["group0"], "team-45");
    assert_eq!(
        browser_group_events[0]["group_properties"]["team"]["members"],
        11
    );

    cleanup(server_handle, pipeline_handle).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flags_accept_evaluation_contexts_and_implicit_properties(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pipeline_endpoint, _pipeline_rx, pipeline_handle) = spawn_pipeline_stub().await?;
    let flags = FeatureFlagStore::from_json(
        r#"{
  "group_type_mapping": { "0": "company" },
  "flags": [
    {
      "key": "env-flag",
      "active": true,
      "evaluation_environments": ["prod"]
    },
    {
      "key": "distinct-flag",
      "active": true,
      "filters": {
        "groups": [
          {
            "properties": [
              { "key": "distinct_id", "value": "ctx-user", "type": "person", "operator": "exact" }
            ],
            "rollout_percentage": 100
          }
        ]
      }
    },
    {
      "key": "group-key-flag",
      "active": true,
      "filters": {
        "aggregation_group_type_index": 0,
        "groups": [
          {
            "properties": [
              { "key": "$group_key", "value": "acme", "type": "group", "operator": "exact" }
            ],
            "rollout_percentage": 100
          }
        ]
      }
    }
  ]
}"#,
    )?;
    let (address, server_handle) =
        spawn_app_with_options(pipeline_endpoint, None, None, None, Some(flags)).await?;

    let base_url = format!("http://{}", address);
    let client = Client::builder().timeout(Duration::from_secs(5)).build()?;
    let response = client
        .post(format!("{}/flags", base_url))
        .json(&json!({
            "distinct_id": "ctx-user",
            "evaluation_contexts": ["prod"],
            "groups": { "company": "acme" },
            "group_properties": {
                "company": { "plan": "pro" }
            }
        }))
        .send()
        .await?;
    assert!(response.status().is_success());
    let body: Value = response.json().await?;
    assert_eq!(body["featureFlags"]["env-flag"], true);
    assert_eq!(body["featureFlags"]["distinct-flag"], true);
    assert_eq!(body["featureFlags"]["group-key-flag"], true);
    assert_eq!(body["errorsWhileComputingFlags"], false);

    let scoped_out = client
        .post(format!("{}/flags", base_url))
        .json(&json!({
            "distinct_id": "ctx-user",
            "evaluation_contexts": ["dev"]
        }))
        .send()
        .await?;
    assert!(scoped_out.status().is_success());
    let scoped_body: Value = scoped_out.json().await?;
    assert!(scoped_body["featureFlags"].get("env-flag").is_none());

    cleanup(server_handle, pipeline_handle).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn posthog_compatibility_endpoints_forward_events() -> Result<(), Box<dyn std::error::Error>>
{
    let (pipeline_endpoint, mut pipeline_rx, pipeline_handle) = spawn_pipeline_stub().await?;
    let (address, server_handle) = spawn_app_with_options(
        pipeline_endpoint,
        Some("phc_project_default".to_string()),
        Some("https://session.example.com".to_string()),
        None,
        None,
    )
    .await?;
    let base_url = format!("http://{}", address);
    let client = Client::builder().timeout(Duration::from_secs(5)).build()?;

    // capture with header api key and sent_at
    let capture_sent_at = Utc::now();
    let capture_payload = json!({
        "event": "integration-capture",
        "distinct_id": "capture-user",
        "properties": {"plan": "pro"},
        "library": "tests"
    });

    let capture_response = client
        .post(format!("{}/capture", base_url))
        .header("x-posthog-api-key", "phc_header_capture")
        .header("x-posthog-sent-at", capture_sent_at.to_rfc3339())
        .json(&capture_payload)
        .send()
        .await?;
    assert!(capture_response.status().is_success());

    let capture_events = wait_for_events(&mut pipeline_rx).await?;
    assert_eq!(capture_events.len(), 1);
    assert_eq!(capture_events[0]["event"], "integration-capture");
    assert_eq!(capture_events[0]["api_key"], "phc_header_capture");
    assert_eq!(capture_events[0]["extra"]["library"], "tests");
    assert_eq!(
        capture_events[0]["extra"]["$sent_at"],
        Value::String(capture_sent_at.to_rfc3339())
    );

    // identify
    let identify_payload = json!({
        "distinct_id": "identify-user",
        "api_key": "phc_identify",
        "properties": {"email": "id@example.com"},
        "context": {"ip": "127.0.0.1"}
    });

    let identify_response = client
        .post(format!("{}/identify", base_url))
        .json(&identify_payload)
        .send()
        .await?;
    assert!(identify_response.status().is_success());

    let identify_events = wait_for_events(&mut pipeline_rx).await?;
    assert_eq!(identify_events.len(), 1);
    assert_eq!(identify_events[0]["event"], "$identify");
    assert_eq!(
        identify_events[0]["person_properties"]["email"],
        "id@example.com"
    );

    // group identify
    let group_payload = json!({
        "group_type": "team",
        "group_key": "team-42",
        "api_key": "phc_group",
        "properties": {"members": 5}
    });

    let group_response = client
        .post(format!("{}/groups", base_url))
        .json(&group_payload)
        .send()
        .await?;
    assert!(group_response.status().is_success());

    let group_events = wait_for_events(&mut pipeline_rx).await?;
    assert_eq!(group_events.len(), 1);
    assert_eq!(group_events[0]["event"], "$groupidentify");
    assert_eq!(group_events[0]["extra"]["group_type"], "team");

    // batch with mixed event types
    let batch_payload = json!({
        "batch": [
            {
                "event": "batched-one",
                "distinct_id": "batch-user-a",
                "properties": {"from": "shared"}
            },
            {
                "event": "$identify",
                "distinct_id": "batch-identify",
                "properties": {"email": "batched@example.com"}
            },
            {
                "type": "alias",
                "distinct_id": "batch-original",
                "alias": "batch-alias"
            },
            {
                "event": "$groupidentify",
                "group_type": "company",
                "group_key": "acme",
                "distinct_id": "ignored"
            }
        ]
    });

    let batch_response = client
        .post(format!("{}/batch", base_url))
        .header("x-posthog-api-key", "phc_batch_header")
        .json(&batch_payload)
        .send()
        .await?;
    assert!(batch_response.status().is_success());

    let batch_events = wait_for_events(&mut pipeline_rx).await?;
    assert_eq!(batch_events.len(), 4);
    assert_eq!(batch_events[0]["event"], "batched-one");
    assert_eq!(batch_events[0]["api_key"], "phc_batch_header");
    assert_eq!(batch_events[1]["event"], "$identify");
    assert_eq!(
        batch_events[1]["person_properties"]["email"],
        "batched@example.com"
    );
    assert_eq!(batch_events[2]["event"], "$create_alias");
    assert_eq!(batch_events[2]["extra"]["alias"], "batch-alias");
    assert_eq!(batch_events[3]["event"], "$groupidentify");
    assert_eq!(batch_events[3]["extra"]["group_type"], "company");

    // alias endpoint
    let alias_payload = json!({
        "distinct_id": "alias-origin",
        "alias": "alias-new"
    });

    let alias_response = client
        .post(format!("{}/alias", base_url))
        .header("x-posthog-api-key", "phc_alias_header")
        .json(&alias_payload)
        .send()
        .await?;
    assert!(alias_response.status().is_success());

    let alias_events = wait_for_events(&mut pipeline_rx).await?;
    assert_eq!(alias_events.len(), 1);
    assert_eq!(alias_events[0]["event"], "$create_alias");
    assert_eq!(alias_events[0]["extra"]["alias"], "alias-new");

    // engage endpoint
    let engage_payload = json!({
        "distinct_id": "people-1",
        "$set": {"name": "Alex"},
        "$unset": ["temp"]
    });

    let engage_response = client
        .post(format!("{}/engage", base_url))
        .header("x-posthog-api-key", "phc_engage_header")
        .json(&engage_payload)
        .send()
        .await?;
    assert!(engage_response.status().is_success());

    let engage_events = wait_for_events(&mut pipeline_rx).await?;
    assert_eq!(engage_events.len(), 1);
    assert_eq!(engage_events[0]["event"], "$engage");
    assert_eq!(engage_events[0]["extra"]["$set"]["name"], "Alex");

    // decide should surface payload token and configured defaults
    let decide_response = client
        .post(format!("{}/decide", base_url))
        .header("x-posthog-api-key", "phc_header_token")
        .json(&json!({ "token": "phc_body_token" }))
        .send()
        .await?;
    assert!(decide_response.status().is_success());
    let decide_body: Value = decide_response.json().await?;
    assert_eq!(decide_body["status"], 200);
    assert_eq!(decide_body["config"]["apiToken"], "phc_body_token");
    assert_eq!(
        decide_body["sessionRecording"]["endpoint"],
        "https://session.example.com"
    );

    let config_response = client
        .get(format!("{}/array/phc_body_token/config", base_url))
        .send()
        .await?;
    assert!(config_response.status().is_success());
    let config_body: Value = config_response.json().await?;
    assert_eq!(config_body["token"], "phc_body_token");
    assert_eq!(config_body["analytics"]["endpoint"], "/e/");
    assert_eq!(config_body["autocaptureExceptions"], true);
    assert_eq!(config_body["errorTracking"]["autocaptureExceptions"], true);
    assert_eq!(
        config_body["errorTracking"]["captureExtensionExceptions"],
        false
    );
    assert_eq!(
        config_body["errorTracking"]["suppressionRules"]
            .as_array()
            .map(Vec::len),
        Some(0)
    );
    assert_eq!(
        config_body["sessionRecording"]["endpoint"],
        "https://session.example.com"
    );
    assert_eq!(
        config_body["sessionRecording"]["scriptConfig"]["script"],
        "posthog-recorder"
    );

    let flags_config_response = client
        .post(format!("{}/flags?config=true", base_url))
        .json(&json!({ "token": "phc_body_token" }))
        .send()
        .await?;
    assert!(flags_config_response.status().is_success());
    let flags_config_body: Value = flags_config_response.json().await?;
    assert_eq!(
        flags_config_body["sessionRecording"]["endpoint"],
        "https://session.example.com"
    );
    assert_eq!(flags_config_body["sessionRecording"]["proxy"], true);

    let config_js_response = client
        .get(format!("{}/array/phc_body_token/config.js", base_url))
        .send()
        .await?;
    assert!(config_js_response.status().is_success());
    let config_js = config_js_response.text().await?;
    assert!(config_js.contains("window._POSTHOG_REMOTE_CONFIG"));
    assert!(config_js.contains("phc_body_token"));

    let exception_script_response = client
        .get(format!(
            "{}/static/1.373.4/exception-autocapture.js",
            base_url
        ))
        .send()
        .await?;
    assert!(exception_script_response.status().is_success());
    let exception_script = exception_script_response.text().await?;
    assert!(exception_script.contains("errorWrappingFunctions"));
    assert!(exception_script.contains("wrapOnError"));

    // session recording ingestion
    let session_payload = json!({
        "event": "$snapshot",
        "properties": {
            "token": "phc_session_chunk",
            "distinct_id": "session-user",
            "$session_id": "session-abc",
            "$window_id": "window-abc",
            "$snapshot_data": [
                {"type": 1, "timestamp": 1700000000000_i64, "data": {"href": "https://example.test"}}
            ],
            "$snapshot_bytes": 123,
            "$lib": "web",
            "$lib_version": "1.0.0"
        }
    });

    let session_response = client
        .post(format!("{}/s", base_url))
        .header("Content-Type", "application/json")
        .json(&session_payload)
        .send()
        .await?;
    assert!(session_response.status().is_success());
    let session_body: Value = session_response.json().await?;
    assert_eq!(session_body["status"], 1);

    let session_events = wait_for_events(&mut pipeline_rx).await?;
    assert_eq!(session_events.len(), 1);
    assert_eq!(session_events[0]["event"], "$snapshot_items");
    assert_eq!(session_events[0]["distinct_id"], "session-user");
    assert_eq!(
        session_events[0]["properties"]["$session_id"],
        "session-abc"
    );
    assert_eq!(
        session_events[0]["properties"]["$snapshot_items"][0]["data"]["href"],
        "https://example.test"
    );
    assert_eq!(session_events[0]["api_key"], "phc_session_chunk");

    let compressed_payload = json!([
        {
            "event": "$snapshot",
            "properties": {
                "token": "phc_session_gzip",
                "distinct_id": "session-user",
                "$session_id": "session-def",
                "$snapshot_data": {"type": 2, "data": {"source": "gzip-js"}},
                "$lib": "web"
            }
        }
    ])
    .to_string();
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(compressed_payload.as_bytes())?;
    let compressed = encoder.finish()?;

    let gzip_response = client
        .post(format!("{}/s?compression=gzip-js", base_url))
        .header("Content-Type", "text/plain")
        .body(compressed)
        .send()
        .await?;
    assert!(gzip_response.status().is_success());

    let gzip_events = wait_for_events(&mut pipeline_rx).await?;
    assert_eq!(gzip_events.len(), 1);
    assert_eq!(gzip_events[0]["event"], "$snapshot_items");
    assert_eq!(gzip_events[0]["properties"]["$session_id"], "session-def");
    assert_eq!(
        gzip_events[0]["properties"]["$snapshot_items"][0]["data"]["source"],
        "gzip-js"
    );
    assert_eq!(gzip_events[0]["api_key"], "phc_session_gzip");

    let beacon_response = client
        .post(format!("{}/s?beacon=1", base_url))
        .json(&json!({
            "event": "$snapshot",
            "properties": {
                "token": "phc_session_beacon",
                "distinct_id": "session-user",
                "$session_id": "session-beacon",
                "$snapshot_data": {"type": 3, "data": {"source": "beacon"}},
                "$lib": "web"
            }
        }))
        .send()
        .await?;
    assert_eq!(beacon_response.status(), StatusCode::NO_CONTENT);

    let beacon_events = wait_for_events(&mut pipeline_rx).await?;
    assert_eq!(beacon_events.len(), 1);
    assert_eq!(beacon_events[0]["event"], "$snapshot_items");
    assert_eq!(
        beacon_events[0]["properties"]["$snapshot_items"][0]["data"]["source"],
        "beacon"
    );
    assert_eq!(beacon_events[0]["api_key"], "phc_session_beacon");

    // health
    let health_response = client.get(format!("{}/healthz", base_url)).send().await?;
    assert!(health_response.status().is_success());
    let health_body: Value = health_response.json().await?;
    assert_eq!(health_body["status"], "ok");

    cleanup(server_handle, pipeline_handle).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_ui_lists_and_loads_session_events() -> Result<(), Box<dyn std::error::Error>> {
    let rows = vec![
        json!({
            "uuid": "chunk-2",
            "event": "$snapshot_items",
            "distinct_id": "replay-user",
            "created_at": "2026-05-22T10:00:02Z",
            "api_key": "phc_replay",
            "properties": {
                "$session_id": "session-1",
                "$snapshot_items": [
                    { "type": 4, "timestamp": 2000, "data": { "href": "https://app.test/page" } }
                ]
            }
        }),
        json!({
            "uuid": "chunk-1",
            "event": "$snapshot_items",
            "distinct_id": "replay-user",
            "created_at": "2026-05-22T10:00:01Z",
            "api_key": "phc_replay",
            "properties": {
                "$session_id": "session-1",
                "$snapshot_items": [
                    { "type": 2, "timestamp": 1000, "data": { "node": { "id": 1 } } }
                ]
            }
        }),
        json!({
            "uuid": "chunk-other",
            "event": "$snapshot",
            "distinct_id": "other-user",
            "created_at": "2026-05-22T10:00:03Z",
            "api_key": "phc_other",
            "properties": {
                "session_id": "session-other",
                "events": [
                    { "type": 4, "timestamp": 3000, "data": {} }
                ]
            }
        }),
        json!({
            "uuid": "event-1",
            "event": "Checkout Started",
            "distinct_id": "replay-user",
            "created_at": "2026-05-22T10:00:02Z",
            "api_key": "phc_replay",
            "properties": {
                "$session_id": "session-1",
                "$current_url": "https://app.test/page",
                "plan": "pro"
            }
        }),
        json!({
            "uuid": "funnel-1",
            "event": "Viewed Pricing",
            "distinct_id": "funnel-user",
            "created_at": "2026-05-22T10:01:00Z",
            "api_key": "phc_replay",
            "properties": {
                "$session_id": "funnel-session",
                "$current_url": "https://app.test/pricing"
            }
        }),
        json!({
            "uuid": "funnel-2",
            "event": "Checkout Started",
            "distinct_id": "funnel-user",
            "created_at": "2026-05-22T10:01:10Z",
            "api_key": "phc_replay",
            "properties": {
                "$session_id": "funnel-session",
                "$current_url": "https://app.test/checkout"
            }
        }),
        json!({
            "uuid": "funnel-3",
            "event": "Viewed Pricing",
            "distinct_id": "stuck-user",
            "created_at": "2026-05-22T10:02:00Z",
            "api_key": "phc_replay",
            "properties": {
                "$session_id": "stuck-session",
                "$current_url": "https://app.test/pricing"
            }
        }),
        json!({
            "uuid": "funnel-4",
            "event": "Viewed Pricing",
            "distinct_id": "stuck-user",
            "created_at": "2026-05-22T10:02:03Z",
            "api_key": "phc_replay",
            "properties": {
                "$session_id": "stuck-session",
                "$current_url": "https://app.test/pricing"
            }
        }),
        json!({
            "uuid": "chunk-friction",
            "event": "$snapshot_items",
            "distinct_id": "friction-user",
            "created_at": "2026-05-22T10:03:00Z",
            "api_key": "phc_replay",
            "properties": {
                "$session_id": "friction-session",
                "$snapshot_items": [
                    { "type": 4, "timestamp": 1000, "data": { "href": "https://app.test/pricing" } },
                    { "type": 3, "timestamp": 1100, "data": { "source": 2, "type": 2, "x": 42, "y": 52 } },
                    { "type": 3, "timestamp": 1700, "data": { "source": 2, "type": 2, "x": 44, "y": 50 } },
                    { "type": 3, "timestamp": 2200, "data": { "source": 2, "type": 2, "x": 43, "y": 51 } },
                    { "type": 3, "timestamp": 40000, "data": { "source": 3, "x": 0, "y": 1600 } }
                ]
            }
        }),
        json!({
            "uuid": "chunk-journey",
            "event": "$snapshot_items",
            "distinct_id": "journey-user",
            "created_at": "2026-05-22T10:04:00Z",
            "api_key": "phc_replay",
            "properties": {
                "$session_id": "journey-session",
                "$snapshot_items": [
                    { "type": 4, "timestamp": 1000, "data": { "href": "https://app.test/start" } }
                ]
            }
        }),
        json!({
            "uuid": "journey-event",
            "event": "Product Viewed",
            "distinct_id": "journey-user",
            "created_at": "2026-05-22T10:04:10Z",
            "api_key": "phc_replay",
            "properties": {
                "$session_id": "journey-session",
                "$current_url": "https://app.test/product",
                "sku": "sku_123"
            }
        }),
    ];

    let (pipeline_endpoint, _pipeline_rx, pipeline_handle) = spawn_pipeline_stub().await?;
    let (replay_endpoint, mut replay_queries, replay_handle) = spawn_replay_sql_stub(rows).await?;
    let replay_config = ReplayConfig::new(
        "account-id".to_string(),
        "bucket-name".to_string(),
        "r2-sql-token".to_string(),
        Some("default.hogflare_events".to_string()),
        Some(500),
        Some(replay_endpoint),
    )?;
    let (address, server_handle) = spawn_app_with_replay(pipeline_endpoint, replay_config).await?;

    let base_url = format!("http://{}", address);
    let client = Client::builder().timeout(Duration::from_secs(5)).build()?;

    let ui_response = client.get(format!("{}/replay", base_url)).send().await?;
    assert!(ui_response.status().is_success());
    let ui = ui_response.text().await?;
    assert!(ui.contains("<title>Hogflare</title>"));
    assert!(ui.contains("aria-label=\"Hogflare\""));
    assert!(ui.contains("id=\"status\" hidden"));
    assert!(ui.contains("Activity timeline"));
    assert!(ui.contains("Date range"));
    assert!(ui.contains("type=\"datetime-local\""));
    assert!(ui.contains("data-date-preset=\"24h\""));
    assert!(ui.contains("data-date-preset=\"30d\" aria-pressed=\"true\""));
    assert!(ui.contains("<option value=\"hour\">Hour</option>"));
    assert!(ui.contains("id=\"clear\" type=\"button\" data-section=\"replays\""));
    assert!(!ui.contains(">Reset<"));
    assert!(ui.contains("aria-label=\"Hide filters\""));
    assert!(ui.contains("aria-label=\"Hide context\""));
    assert!(ui.contains(">Search<"));
    assert!(ui.contains("Funnel steps"));
    assert!(ui.contains("Friction signal"));
    assert!(ui.contains("Person journey"));
    assert!(ui.contains("rrweb-player"));
    assert!(!ui.contains("Raw summary"));

    let sessions_response = client
        .get(format!("{}/replay/api/sessions", base_url))
        .query(&[
            ("api_key", "phc_replay"),
            ("distinct_id", "replay-user"),
            ("limit", "25"),
        ])
        .send()
        .await?;
    assert!(sessions_response.status().is_success());
    let sessions: Value = sessions_response.json().await?;
    assert_eq!(sessions["sessions"].as_array().unwrap().len(), 1);
    assert_eq!(sessions["sessions"][0]["session_id"], "session-1");
    assert_eq!(sessions["sessions"][0]["chunk_count"], 2);
    assert_eq!(sessions["sessions"][0]["event_count"], 2);
    assert_eq!(sessions["sessions"][0]["duration_ms"], 1000);

    let first_query = tokio::time::timeout(Duration::from_secs(2), replay_queries.recv())
        .await?
        .expect("expected replay sql query");
    assert!(first_query.contains("from default.hogflare_events"));
    assert!(first_query.contains("api_key = 'phc_replay'"));
    assert!(first_query.contains("distinct_id = 'replay-user'"));
    assert!(first_query.contains("event in ('$snapshot', '$snapshot_items')"));

    let replay_events_response = client
        .get(format!("{}/replay/api/events", base_url))
        .query(&[
            ("api_key", "phc_replay"),
            ("distinct_id", "replay-user"),
            ("event_name", "Checkout Started"),
            ("url", "page"),
        ])
        .send()
        .await?;
    assert!(replay_events_response.status().is_success());
    let replay_events: Value = replay_events_response.json().await?;
    assert_eq!(replay_events["events"].as_array().unwrap().len(), 1);
    assert_eq!(replay_events["events"][0]["event"], "Checkout Started");
    assert_eq!(replay_events["events"][0]["session_id"], "session-1");
    assert_eq!(replay_events["events"][0]["url"], "https://app.test/page");
    assert_eq!(
        replay_events["events"][0]["properties"][0]["key"],
        "$current_url"
    );

    let event_query = tokio::time::timeout(Duration::from_secs(2), replay_queries.recv())
        .await?
        .expect("expected replay event sql query");
    assert!(event_query.contains("event not in ('$snapshot', '$snapshot_items')"));
    assert!(event_query.contains("event = 'Checkout Started'"));

    let events_response = client
        .get(format!("{}/replay/api/sessions/session-1", base_url))
        .query(&[
            ("api_key", "phc_replay"),
            ("distinct_id", "replay-user"),
            ("at_ms", "250"),
        ])
        .send()
        .await?;
    assert!(events_response.status().is_success());
    let events: Value = events_response.json().await?;
    assert_eq!(events["session"]["session_id"], "session-1");
    assert_eq!(events["chunks"].as_array().unwrap().len(), 2);
    assert_eq!(events["events"].as_array().unwrap().len(), 2);
    assert_eq!(events["activity"].as_array().unwrap().len(), 2);
    assert_eq!(events["replay_anchor_ms"], 250);
    assert_eq!(events["events"][0]["timestamp"], 1000);
    assert_eq!(events["events"][1]["timestamp"], 2000);

    let funnel_response = client
        .get(format!("{}/replay/api/funnels", base_url))
        .query(&[
            ("api_key", "phc_replay"),
            ("steps", "Viewed Pricing,Checkout Started"),
            ("limit", "25"),
        ])
        .send()
        .await?;
    assert!(funnel_response.status().is_success());
    let funnel: Value = funnel_response.json().await?;
    assert_eq!(funnel["steps"].as_array().unwrap().len(), 2);
    assert!(funnel["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .any(
            |session| session["session_id"] == "funnel-session" && session["status"] == "converted"
        ));
    assert!(funnel["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|session| session["session_id"] == "stuck-session" && session["status"] == "stuck"));

    let friction_response = client
        .get(format!("{}/replay/api/friction", base_url))
        .query(&[("api_key", "phc_replay"), ("distinct_id", "friction-user")])
        .send()
        .await?;
    assert!(friction_response.status().is_success());
    let friction: Value = friction_response.json().await?;
    assert_eq!(friction["sessions"].as_array().unwrap().len(), 1);
    assert!(friction["sessions"][0]["signals"]
        .as_array()
        .unwrap()
        .iter()
        .any(|signal| signal["kind"] == "rage_click"));

    let person_response = client
        .get(format!("{}/replay/api/person", base_url))
        .query(&[("api_key", "phc_replay"), ("distinct_id", "journey-user")])
        .send()
        .await?;
    assert!(person_response.status().is_success());
    let person: Value = person_response.json().await?;
    assert_eq!(person["sessions"].as_array().unwrap().len(), 1);
    assert_eq!(person["events"].as_array().unwrap().len(), 1);
    assert!(person["timeline"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item["kind"] == "event" && item["title"] == "Product Viewed"));

    cleanup(server_handle, pipeline_handle).await;
    replay_handle.abort();
    let _ = replay_handle.await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn anonymous_identify_transition_enriches_events_and_person(
) -> Result<(), Box<dyn std::error::Error>> {
    let debug_token = "debug-transition-token";
    let (pipeline_endpoint, mut pipeline_rx, pipeline_handle) = spawn_pipeline_stub().await?;
    let (address, server_handle) = spawn_app_with_options_and_debug(
        pipeline_endpoint,
        None,
        None,
        None,
        None,
        Some(debug_token.to_string()),
    )
    .await?;
    let base_url = format!("http://{}", address);
    let client = Client::builder().timeout(Duration::from_secs(5)).build()?;

    let anon_capture = json!({
        "event": "anon-pageview",
        "distinct_id": "anon-transition-user",
        "api_key": "phc_people_transition",
        "properties": {
            "$set": { "initial_referrer": "adwords", "anon_trait": "curious" },
            "$set_once": { "first_seen_source": "landing-page" },
            "page": "/landing"
        }
    });

    client
        .post(format!("{}/capture", base_url))
        .json(&anon_capture)
        .send()
        .await?
        .error_for_status()?;

    let anon_events = wait_for_events(&mut pipeline_rx).await?;
    let anon_event = anon_events
        .first()
        .expect("expected anonymous capture event");
    assert_eq!(anon_event["event"], "anon-pageview");
    assert_eq!(anon_event["distinct_id"], "anon-transition-user");
    assert_eq!(anon_event["api_key"], "phc_people_transition");
    let anon_person_id = anon_event["person_id"]
        .as_str()
        .expect("anonymous event should include person_id")
        .to_string();
    assert!(
        anon_event["person_created_at"].as_str().is_some(),
        "anonymous event should include person_created_at"
    );
    assert_eq!(
        anon_event["person_properties"]["initial_referrer"],
        "adwords"
    );
    assert_eq!(
        anon_event["person_properties"]["first_seen_source"],
        "landing-page"
    );

    let identify_payload = json!({
        "distinct_id": "identified-transition-user",
        "api_key": "phc_people_transition",
        "properties": {
            "$anon_distinct_id": "anon-transition-user",
            "$set": {
                "email": "transition@example.com",
                "plan": "pro"
            },
            "$set_once": {
                "signup_source": "product"
            }
        }
    });

    client
        .post(format!("{}/identify", base_url))
        .json(&identify_payload)
        .send()
        .await?
        .error_for_status()?;

    let identify_events = wait_for_events(&mut pipeline_rx).await?;
    let identify_event = identify_events.first().expect("expected identify event");
    assert_eq!(identify_event["event"], "$identify");
    assert_eq!(identify_event["distinct_id"], "identified-transition-user");
    assert_eq!(identify_event["person_id"], anon_person_id);
    assert_eq!(
        identify_event["person_properties"]["email"],
        "transition@example.com"
    );
    assert_eq!(identify_event["person_properties"]["plan"], "pro");
    assert_eq!(
        identify_event["person_properties"]["initial_referrer"],
        "adwords"
    );
    assert_eq!(
        identify_event["person_properties"]["first_seen_source"],
        "landing-page"
    );
    assert_eq!(
        identify_event["person_properties"]["signup_source"],
        "product"
    );

    client
        .post(format!("{}/capture", base_url))
        .json(&json!({
            "event": "identified-action",
            "distinct_id": "identified-transition-user",
            "api_key": "phc_people_transition",
            "properties": { "button": "checkout" }
        }))
        .send()
        .await?
        .error_for_status()?;

    let identified_events = wait_for_events(&mut pipeline_rx).await?;
    let identified_event = identified_events
        .first()
        .expect("expected identified capture event");
    assert_eq!(identified_event["event"], "identified-action");
    assert_eq!(
        identified_event["distinct_id"],
        "identified-transition-user"
    );
    assert_eq!(identified_event["person_id"], anon_person_id);
    assert_eq!(
        identified_event["person_properties"]["email"],
        "transition@example.com"
    );
    assert_eq!(identified_event["person_properties"]["plan"], "pro");
    assert_eq!(
        identified_event["person_properties"]["initial_referrer"],
        "adwords"
    );

    let person_response = client
        .get(format!("{}/__debug/person/anon-transition-user", base_url))
        .header("x-hogflare-debug-token", debug_token)
        .send()
        .await?;
    assert_eq!(person_response.status(), StatusCode::OK);
    let person: Value = person_response.json().await?;
    assert_eq!(person["canonical_id"], "identified-transition-user");
    assert_eq!(person["record"]["uuid"], anon_person_id);
    assert_eq!(
        person["record"]["properties"]["email"],
        "transition@example.com"
    );
    assert_eq!(person["record"]["properties"]["plan"], "pro");
    assert_eq!(
        person["record"]["properties"]["initial_referrer"],
        "adwords"
    );
    assert_eq!(
        person["record"]["properties_set_once"]["first_seen_source"],
        "landing-page"
    );
    assert_eq!(
        person["record"]["properties_set_once"]["signup_source"],
        "product"
    );
    let distinct_ids = person["record"]["distinct_ids"]
        .as_array()
        .expect("person should include distinct_ids");
    assert!(distinct_ids.contains(&Value::String("anon-transition-user".to_string())));
    assert!(distinct_ids.contains(&Value::String("identified-transition-user".to_string())));

    cleanup(server_handle, pipeline_handle).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sdk_config_disables_session_recording_without_endpoint(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pipeline_endpoint, _pipeline_rx, pipeline_handle) = spawn_pipeline_stub().await?;
    let (address, server_handle) =
        spawn_app_with_options(pipeline_endpoint, None, None, None, None).await?;

    let base_url = format!("http://{}", address);
    let client = Client::builder().timeout(Duration::from_secs(5)).build()?;

    let decide_response = client
        .post(format!("{}/decide", base_url))
        .json(&json!({ "token": "phc_no_replay" }))
        .send()
        .await?;
    assert!(decide_response.status().is_success());
    let decide_body: Value = decide_response.json().await?;
    assert_eq!(decide_body["sessionRecording"], false);

    let config_response = client
        .get(format!("{}/array/phc_no_replay/config", base_url))
        .send()
        .await?;
    assert!(config_response.status().is_success());
    let config_body: Value = config_response.json().await?;
    assert_eq!(config_body["sessionRecording"], false);

    let flags_config_response = client
        .post(format!("{}/flags?config=true", base_url))
        .json(&json!({ "token": "phc_no_replay" }))
        .send()
        .await?;
    assert!(flags_config_response.status().is_success());
    let flags_config_body: Value = flags_config_response.json().await?;
    assert_eq!(flags_config_body["sessionRecording"], false);

    cleanup(server_handle, pipeline_handle).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persons_pipeline_receives_anon_to_identified_snapshots(
) -> Result<(), Box<dyn std::error::Error>> {
    let debug_token = "debug-persons-pipeline-token";
    let (pipeline_endpoint, mut pipeline_rx, pipeline_handle) = spawn_pipeline_stub().await?;
    let (persons_endpoint, mut persons_rx, persons_handle) = spawn_pipeline_stub().await?;
    let (address, server_handle) = spawn_app_with_options_debug_and_person_pipeline(
        pipeline_endpoint,
        Some(persons_endpoint),
        None,
        None,
        None,
        None,
        Some(debug_token.to_string()),
    )
    .await?;
    let base_url = format!("http://{}", address);
    let client = Client::builder().timeout(Duration::from_secs(5)).build()?;

    client
        .post(format!("{}/capture", base_url))
        .json(&json!({
            "event": "persons-anon-capture",
            "distinct_id": "persons-anon-id",
            "api_key": "phc_persons_pipeline",
            "properties": {
                "$set": { "initial_referrer": "paid-search" },
                "$set_once": { "first_seen_source": "landing" },
                "page": "/start"
            }
        }))
        .send()
        .await?
        .error_for_status()?;

    let anon_people = wait_for_events(&mut persons_rx).await?;
    let anon_events = wait_for_events(&mut pipeline_rx).await?;
    let anon_person = anon_people
        .first()
        .expect("expected anonymous person snapshot");
    let anon_event = anon_events.first().expect("expected anonymous event");
    assert_eq!(anon_person["operation"], "capture");
    assert_eq!(anon_person["canonical_distinct_id"], "persons-anon-id");
    assert_eq!(anon_person["api_key"], "phc_persons_pipeline");
    assert_eq!(anon_person["source_event_uuid"], anon_event["uuid"]);
    let person_id = anon_person["person_id"]
        .as_str()
        .expect("person snapshot should include person_id")
        .to_string();
    assert_eq!(anon_event["person_id"], person_id);
    assert_json_array_contains(&anon_person["distinct_ids"], "persons-anon-id");
    assert_eq!(anon_person["properties"]["initial_referrer"], "paid-search");
    assert_eq!(
        anon_person["properties_set_once"]["first_seen_source"],
        "landing"
    );
    assert_eq!(
        anon_person["merged_properties"]["initial_referrer"],
        "paid-search"
    );
    assert_eq!(
        anon_person["merged_properties"]["first_seen_source"],
        "landing"
    );

    client
        .post(format!("{}/identify", base_url))
        .json(&json!({
            "distinct_id": "persons-identified-id",
            "api_key": "phc_persons_pipeline",
            "properties": {
                "$anon_distinct_id": "persons-anon-id",
                "$set": {
                    "email": "persons@example.com",
                    "plan": "pro"
                },
                "$set_once": {
                    "signup_source": "checkout"
                }
            }
        }))
        .send()
        .await?
        .error_for_status()?;

    let identified_people = wait_for_events(&mut persons_rx).await?;
    let identify_events = wait_for_events(&mut pipeline_rx).await?;
    let identified_person = identified_people
        .first()
        .expect("expected identified person snapshot");
    let identify_event = identify_events.first().expect("expected identify event");
    assert_eq!(identified_person["operation"], "identify");
    assert_eq!(
        identified_person["canonical_distinct_id"],
        "persons-identified-id"
    );
    assert_eq!(identified_person["person_id"], person_id);
    assert_eq!(
        identified_person["source_event_uuid"],
        identify_event["uuid"]
    );
    assert_json_array_contains(&identified_person["distinct_ids"], "persons-anon-id");
    assert_json_array_contains(&identified_person["distinct_ids"], "persons-identified-id");
    assert_eq!(
        identified_person["properties"]["email"],
        "persons@example.com"
    );
    assert_eq!(identified_person["properties"]["plan"], "pro");
    assert_eq!(
        identified_person["properties"]["initial_referrer"],
        "paid-search"
    );
    assert_eq!(
        identified_person["properties_set_once"]["first_seen_source"],
        "landing"
    );
    assert_eq!(
        identified_person["properties_set_once"]["signup_source"],
        "checkout"
    );
    assert_eq!(
        identified_person["merged_properties"]["signup_source"],
        "checkout"
    );

    client
        .post(format!("{}/capture", base_url))
        .json(&json!({
            "event": "persons-identified-capture",
            "distinct_id": "persons-identified-id",
            "api_key": "phc_persons_pipeline",
            "properties": { "button": "pay" }
        }))
        .send()
        .await?
        .error_for_status()?;

    let final_people = wait_for_events(&mut persons_rx).await?;
    let final_events = wait_for_events(&mut pipeline_rx).await?;
    let final_person = final_people
        .first()
        .expect("expected final person snapshot");
    let final_event = final_events.first().expect("expected final event");
    assert_eq!(final_person["operation"], "capture");
    assert_eq!(final_person["person_id"], person_id);
    assert_eq!(final_person["source_event_uuid"], final_event["uuid"]);
    assert_eq!(
        final_person["canonical_distinct_id"],
        "persons-identified-id"
    );
    assert_eq!(
        final_person["merged_properties"]["email"],
        "persons@example.com"
    );
    assert_json_array_contains(&final_person["distinct_ids"], "persons-anon-id");
    assert_json_array_contains(&final_person["distinct_ids"], "persons-identified-id");

    cleanup(server_handle, pipeline_handle).await;
    persons_handle.abort();
    let _ = persons_handle.await;
    Ok(())
}

fn assert_json_array_contains(value: &Value, expected: &str) {
    let values = value.as_array().expect("expected JSON array");
    assert!(
        values.contains(&Value::String(expected.to_string())),
        "expected {values:?} to contain {expected:?}"
    );
}
