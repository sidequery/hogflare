#[path = "helpers/mod.rs"]
mod helpers;

use std::{io::Write, time::Duration};

use helpers::{
    cleanup, collect_events_until, spawn_app, spawn_app_with_options,
    spawn_app_with_options_and_debug, spawn_app_with_runtime_options, spawn_pipeline_stub,
    start_docker_pipeline, stop_docker_pipeline, wait_for_events, wait_for_pipeline_events,
};
use hogflare::groups::GroupTypeMap;
use reqwest::Client;
use serde_json::Value;
use tempfile::NamedTempFile;
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
    assert_eq!(event["api_key"], "phc_test_integration_key");

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
async fn posthog_js_capture_exception_is_forwarded_to_pipeline(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pipeline_endpoint, mut pipeline_rx, pipeline_handle) = spawn_pipeline_stub().await?;
    let (address, server_handle) = spawn_app(pipeline_endpoint).await?;

    let status = Command::new("bun")
        .arg("run")
        .arg("posthog_exception.js")
        .current_dir("tests/js_client")
        .env("HOGFLARE_HOST", format!("http://{}", address))
        .env("HOGFLARE_API_KEY", "phc_test_integration_key")
        .env("HOGFLARE_DISTINCT_ID", "js-exception-user")
        .status()
        .await?;

    if !status.success() {
        return Err(format!("posthog exception script exited with status {status:?}").into());
    }

    let events = wait_for_events(&mut pipeline_rx).await?;
    let event = events
        .iter()
        .find(|event| event["event"] == "$exception")
        .expect("expected $exception event in pipeline payload");

    assert_eq!(event["source"], "posthog");
    assert_eq!(event["distinct_id"], "js-exception-user");
    assert_eq!(event["api_key"], "phc_test_integration_key");

    let properties = event
        .get("properties")
        .and_then(Value::as_object)
        .expect("$exception event should include properties");
    assert_eq!(
        properties.get("component").and_then(Value::as_str),
        Some("checkout")
    );
    assert_eq!(
        properties.get("severity").and_then(Value::as_str),
        Some("high")
    );

    let exception = properties["$exception_list"]
        .as_array()
        .and_then(|exceptions| exceptions.first())
        .and_then(Value::as_object)
        .expect("captureException should send $exception_list");
    assert_eq!(
        exception.get("type").and_then(Value::as_str),
        Some("TypeError")
    );
    assert_eq!(
        exception.get("value").and_then(Value::as_str),
        Some("checkout total was NaN")
    );
    assert_eq!(
        exception["mechanism"]
            .get("handled")
            .and_then(Value::as_bool),
        Some(true)
    );
    assert!(
        exception["stacktrace"]["frames"]
            .as_array()
            .map(|frames| !frames.is_empty())
            .unwrap_or(false),
        "captureException should include stack frames"
    );
    assert!(
        properties["$exception_steps"]
            .as_array()
            .map(|steps| !steps.is_empty())
            .unwrap_or(false),
        "exception steps should be attached when configured"
    );

    cleanup(server_handle, pipeline_handle).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn posthog_js_auto_exception_capture_is_forwarded_to_pipeline(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pipeline_endpoint, mut pipeline_rx, pipeline_handle) = spawn_pipeline_stub().await?;
    let (address, server_handle) = spawn_app(pipeline_endpoint).await?;

    let status = Command::new("bun")
        .arg("run")
        .arg("posthog_auto_exception.js")
        .current_dir("tests/js_client")
        .env("HOGFLARE_HOST", format!("http://{}", address))
        .env("HOGFLARE_API_KEY", "phc_test_integration_key")
        .env("HOGFLARE_DISTINCT_ID", "js-auto-exception-user")
        .status()
        .await?;

    if !status.success() {
        return Err(format!("posthog auto exception script exited with status {status:?}").into());
    }

    let events = wait_for_events(&mut pipeline_rx).await?;
    let event = events
        .iter()
        .find(|event| event["event"] == "$exception")
        .expect("expected auto-captured $exception event in pipeline payload");

    assert_eq!(event["source"], "posthog");
    assert_eq!(event["distinct_id"], "js-auto-exception-user");
    assert_eq!(event["api_key"], "phc_test_integration_key");

    let exception = event["properties"]["$exception_list"]
        .as_array()
        .and_then(|exceptions| exceptions.first())
        .and_then(Value::as_object)
        .expect("auto capture should send $exception_list");
    assert_eq!(
        exception.get("type").and_then(Value::as_str),
        Some("RangeError")
    );
    assert_eq!(
        exception.get("value").and_then(Value::as_str),
        Some("auto captured checkout failure")
    );
    assert_eq!(
        exception["mechanism"]
            .get("handled")
            .and_then(Value::as_bool),
        Some(false)
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
            assert_eq!(event["api_key"], "phc_test_integration_key");
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
    assert_eq!(event["api_key"], "phc_test_integration_key");

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
async fn posthog_js_identify_aliases_anonymous_id_with_sdk_payload(
) -> Result<(), Box<dyn std::error::Error>> {
    let debug_token = "debug-js-sdk-identify";
    let anon_id = "anonymous-sdk-user";
    let identified_id = "identified-sdk-user";
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

    let status = Command::new("bun")
        .arg("run")
        .arg("posthog_identify_alias.js")
        .current_dir("tests/js_client")
        .env("HOGFLARE_HOST", format!("http://{}", address))
        .env("HOGFLARE_API_KEY", "phc_test_integration_key")
        .env("HOGFLARE_ANON_DISTINCT_ID", anon_id)
        .env("HOGFLARE_IDENTIFIED_ID", identified_id)
        .status()
        .await?;

    if !status.success() {
        return Err(format!("posthog identify alias script exited with status {status:?}").into());
    }

    let events = collect_events_until(&mut pipeline_rx, 1, Duration::from_secs(10)).await?;
    let event = events
        .iter()
        .find(|e| e["event"] == "$identify")
        .expect("expected $identify event in pipeline payload");

    assert_eq!(event["source"], "posthog");
    assert_eq!(event["distinct_id"], identified_id);
    assert_eq!(
        event["person_properties"]["email"],
        "sdk-identify@example.com"
    );

    let client = Client::builder().timeout(Duration::from_secs(5)).build()?;
    let snapshot: Value = client
        .get(format!("http://{}/__debug/person/{}", address, anon_id))
        .header("x-hogflare-debug-token", debug_token)
        .send()
        .await?
        .json()
        .await?;

    assert_eq!(snapshot["canonical_id"], identified_id);
    assert_eq!(
        snapshot["record"]["properties"]["email"],
        "sdk-identify@example.com"
    );

    cleanup(server_handle, pipeline_handle).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn posthog_js_identify_transitions_anonymous_person() -> Result<(), Box<dyn std::error::Error>>
{
    let debug_token = "debug-js-transition-token";
    let anon_id = "js-anon-transition-user";
    let identified_id = "js-identified-transition-user";
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

    let status = Command::new("bun")
        .arg("run")
        .arg("posthog_identify_transition.js")
        .current_dir("tests/js_client")
        .env("HOGFLARE_HOST", format!("http://{}", address))
        .env("HOGFLARE_API_KEY", "phc_test_integration_key")
        .env("HOGFLARE_DISTINCT_ID", anon_id)
        .env("HOGFLARE_IDENTIFIED_ID", identified_id)
        .status()
        .await?;

    if !status.success() {
        return Err(format!("posthog transition script exited with status {status:?}").into());
    }

    let mut all_events: Vec<Value> = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while all_events.len() < 3 && tokio::time::Instant::now() < deadline {
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

    let find_event = |event_name: &str| -> &Value {
        all_events
            .iter()
            .find(|event| event["event"] == event_name)
            .unwrap_or_else(|| panic!("missing event {event_name}: {all_events:?}"))
    };

    let anon_event = find_event("js-anon-pageview");
    assert_eq!(anon_event["distinct_id"], anon_id);
    assert_eq!(anon_event["api_key"], "phc_test_integration_key");
    let person_id = anon_event["person_id"]
        .as_str()
        .expect("anonymous SDK event should include person_id");
    assert_eq!(
        anon_event["person_properties"]["initial_referrer"],
        "adwords"
    );
    assert_eq!(
        anon_event["person_properties"]["first_seen_source"],
        "landing-page"
    );

    let identify_event = find_event("$identify");
    assert_eq!(identify_event["distinct_id"], identified_id);
    assert_eq!(identify_event["api_key"], "phc_test_integration_key");
    assert_eq!(identify_event["person_id"], person_id);
    assert_eq!(
        identify_event["person_properties"]["email"],
        "js-transition@example.com"
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

    let identified_event = find_event("js-identified-action");
    assert_eq!(identified_event["distinct_id"], identified_id);
    assert_eq!(identified_event["api_key"], "phc_test_integration_key");
    assert_eq!(identified_event["person_id"], person_id);
    assert_eq!(
        identified_event["person_properties"]["email"],
        "js-transition@example.com"
    );
    assert_eq!(
        identified_event["person_properties"]["initial_referrer"],
        "adwords"
    );

    let client = Client::builder().timeout(Duration::from_secs(2)).build()?;
    let person_response = client
        .get(format!("http://{}/__debug/person/{}", address, anon_id))
        .header("x-hogflare-debug-token", debug_token)
        .send()
        .await?;
    assert!(person_response.status().is_success());
    let person: Value = person_response.json().await?;
    assert_eq!(person["canonical_id"], identified_id);
    assert_eq!(person["record"]["uuid"], person_id);
    assert_eq!(
        person["record"]["properties"]["email"],
        "js-transition@example.com"
    );
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
async fn posthog_js_compressed_group_flow_hydrates_followup_capture(
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
        .arg("posthog_compressed_group_flow.js")
        .current_dir("tests/js_client")
        .env("HOGFLARE_HOST", format!("http://{}", address))
        .env("HOGFLARE_API_KEY", "phc_test_integration_key")
        .env("HOGFLARE_DISTINCT_ID", "js-compressed-group-user")
        .status()
        .await?;

    if !status.success() {
        return Err(
            format!("posthog compressed group script exited with status {status:?}").into(),
        );
    }

    let events = collect_events_until(&mut pipeline_rx, 2, Duration::from_secs(10)).await?;
    let group_event = events
        .iter()
        .find(|e| e["event"] == "$groupidentify")
        .expect("expected $groupidentify event in pipeline payload");
    assert_eq!(group_event["source"], "posthog");
    assert_eq!(group_event["extra"]["group_type"], "company");
    assert_eq!(group_event["extra"]["group_key"], "sdk-acme");
    assert_eq!(group_event["group0"], "sdk-acme");
    assert_eq!(
        group_event["group_properties"]["company"]["plan"],
        "enterprise"
    );
    assert_eq!(group_event["group_properties"]["company"]["seats"], 42);

    let capture_event = events
        .iter()
        .find(|e| e["event"] == "js-grouped-capture")
        .expect("expected grouped capture event in pipeline payload");
    assert_eq!(capture_event["source"], "posthog");
    assert_eq!(capture_event["distinct_id"], "js-compressed-group-user");
    assert_eq!(capture_event["group0"], "sdk-acme");
    assert_eq!(
        capture_event["group_properties"]["company"]["plan"],
        "enterprise"
    );
    assert_eq!(capture_event["properties"]["client"], "posthog-js");

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn posthog_js_session_replay_output_is_playable() -> Result<(), Box<dyn std::error::Error>> {
    let api_key = "phc_replay_playback";
    let distinct_id = "js-replay-playback-user";
    let (pipeline_endpoint, mut pipeline_rx, pipeline_handle) = spawn_pipeline_stub().await?;
    let (address, server_handle) = spawn_app_with_options(
        pipeline_endpoint,
        Some(api_key.to_string()),
        Some("/s/".to_string()),
        None,
        None,
    )
    .await?;

    let record_output = Command::new("bun")
        .arg("run")
        .arg("posthog_session_replay_playwright.js")
        .current_dir("tests/js_client")
        .env("HOGFLARE_HOST", format!("http://{}", address))
        .env("HOGFLARE_API_KEY", api_key)
        .env("HOGFLARE_DISTINCT_ID", distinct_id)
        .output()
        .await?;

    if !record_output.status.success() {
        return Err(format!(
            "posthog replay script exited with status {:?}\nstdout:\n{}\nstderr:\n{}",
            record_output.status,
            String::from_utf8_lossy(&record_output.stdout),
            String::from_utf8_lossy(&record_output.stderr)
        )
        .into());
    }

    let events = collect_events_until(&mut pipeline_rx, 1, Duration::from_secs(15)).await?;
    let replay_event = events
        .iter()
        .find(|event| event["event"] == "$snapshot_items")
        .ok_or_else(|| format!("missing $snapshot_items event: {events:?}"))?;

    assert_eq!(replay_event["source"], "posthog");
    assert_eq!(replay_event["api_key"], api_key);
    assert_eq!(replay_event["distinct_id"], distinct_id);
    assert_eq!(replay_event["properties"]["distinct_id"], distinct_id);
    assert_eq!(replay_event["properties"]["$lib"], "web");

    let snapshot_items = replay_event["properties"]["$snapshot_items"]
        .as_array()
        .ok_or("expected $snapshot_items array")?;
    assert!(
        snapshot_items.len() >= 2,
        "expected at least a full snapshot and one follow-up event, got {snapshot_items:?}"
    );
    assert!(
        snapshot_items.iter().any(|event| event["type"] == 2),
        "expected replay output to contain an rrweb full snapshot: {snapshot_items:?}"
    );

    let mut replay_file = NamedTempFile::new()?;
    replay_file.write_all(serde_json::to_string(snapshot_items)?.as_bytes())?;
    replay_file.flush()?;

    let playback_output = Command::new("bun")
        .arg("run")
        .arg("verify_rrweb_playback.js")
        .current_dir("tests/js_client")
        .env("HOGFLARE_REPLAY_EVENTS_FILE", replay_file.path())
        .output()
        .await?;

    if !playback_output.status.success() {
        return Err(format!(
            "rrweb playback script exited with status {:?}\nrecord stdout:\n{}\nrecord stderr:\n{}\nplayback stdout:\n{}\nplayback stderr:\n{}",
            playback_output.status,
            String::from_utf8_lossy(&record_output.stdout),
            String::from_utf8_lossy(&record_output.stderr),
            String::from_utf8_lossy(&playback_output.stdout),
            String::from_utf8_lossy(&playback_output.stderr)
        )
        .into());
    }

    cleanup(server_handle, pipeline_handle).await;
    Ok(())
}
