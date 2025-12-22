#[path = "helpers/mod.rs"]
mod helpers;

use chrono::Utc;
use helpers::{cleanup, spawn_app_with_options, spawn_pipeline_stub, wait_for_events};
use hmac::{Hmac, Mac};
use reqwest::{Client, StatusCode};
use serde_json::{json, Value};
use sha2::Sha256;
use std::time::Duration;

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

    let mut mac = Hmac::<Sha256>::new_from_slice(signing_secret.as_bytes()).unwrap();
    mac.update(body.as_bytes());
    let signature = hex::encode(mac.finalize().into_bytes());

    let signed = client
        .post(format!("{}/capture", base_url))
        .header("Content-Type", "application/json")
        .header("X-POSTHOG-API-KEY", "phc_signature")
        .header("X-POSTHOG-SIGNATURE", format!("sha256={}", signature))
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

    // session recording ingestion stubs
    let session_payload = json!({
        "data": {
            "chunk": "base64-chunk",
            "metadata": {"distinct_id": "session-user"}
        },
        "token": "phc_session_chunk"
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
    assert_eq!(session_events[0]["event"], "$snapshot");
    assert_eq!(
        session_events[0]["properties"]["data"]["metadata"]["distinct_id"],
        "session-user"
    );
    assert_eq!(session_events[0]["api_key"], "phc_session_chunk");

    // health
    let health_response = client.get(format!("{}/healthz", base_url)).send().await?;
    assert!(health_response.status().is_success());
    let health_body: Value = health_response.json().await?;
    assert_eq!(health_body["status"], "ok");

    cleanup(server_handle, pipeline_handle).await;
    Ok(())
}
