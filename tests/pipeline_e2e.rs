#[path = "helpers/mod.rs"]
mod helpers;

use std::time::Duration;

use helpers::{
    spawn_app_with_options, start_docker_pipeline, stop_docker_pipeline, wait_for_pipeline_events,
};
use reqwest::Client;
use serde_json::{json, Value};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pipeline_handles_capture_batch_alias_and_session() -> Result<(), Box<dyn std::error::Error>>
{
    let (pipeline_base, _pipeline_guard) = start_docker_pipeline().await?;

    let test_result = async {
        let (address, server_handle) = spawn_app_with_options(
            pipeline_base.clone(),
            Some("phc_default".to_string()),
            Some("https://session.example.com".to_string()),
            None,
        )
        .await?;

        let client = Client::builder().timeout(Duration::from_secs(2)).build()?;
        let hogflare_base = format!("http://{}", address);

        // Capture
        client
            .post(format!("{}/capture", hogflare_base))
            .header("X-POSTHOG-API-KEY", "phc_e2e")
            .json(&json!({
                "event": "e2e-capture",
                "distinct_id": "user-1",
                "properties": {"source": "direct"}
            }))
            .send()
            .await?
            .error_for_status()?;

        // Identify
        client
            .post(format!("{}/identify", hogflare_base))
            .json(&json!({
                "distinct_id": "user-1",
                "properties": {"email": "user1@example.com"}
            }))
            .send()
            .await?
            .error_for_status()?;

        // Group identify
        client
            .post(format!("{}/groups", hogflare_base))
            .json(&json!({
                "group_type": "team",
                "group_key": "team-42",
                "properties": {"members": 3}
            }))
            .send()
            .await?
            .error_for_status()?;

        // Alias
        client
            .post(format!("{}/alias", hogflare_base))
            .json(&json!({
                "distinct_id": "user-1",
                "alias": "user-1-alias"
            }))
            .send()
            .await?
            .error_for_status()?;

        // Engage
        client
            .post(format!("{}/engage", hogflare_base))
            .json(&json!({
                "distinct_id": "user-1",
                "$set": {"plan": "pro"},
                "$unset": ["temp"]
            }))
            .send()
            .await?
            .error_for_status()?;

        // Batch with mixed events
        client
            .post(format!("{}/batch", hogflare_base))
            .header("X-POSTHOG-API-KEY", "phc_batch")
            .json(&json!({
                "batch": [
                    {
                        "event": "e2e-batch-capture",
                        "distinct_id": "user-2",
                        "properties": {"batch": true}
                    },
                    {
                        "type": "alias",
                        "distinct_id": "user-2",
                        "alias": "user-2-alias"
                    },
                    {
                        "event": "$identify",
                        "distinct_id": "user-2",
                        "properties": {"email": "batch@example.com"}
                    }
                ]
            }))
            .send()
            .await?
            .error_for_status()?;

        // Session recording snapshot
        client
            .post(format!("{}/s", hogflare_base))
            .json(&json!({
                "token": "phc_session",
                "data": {
                    "metadata": {"distinct_id": "user-1"},
                    "chunk": "base64-chunk"
                }
            }))
            .send()
            .await?
            .error_for_status()?;

        let events_url = pipeline_base.join("events")?;
        let events = wait_for_pipeline_events(&client, &events_url, 9).await?;
        assert_eq!(events.len(), 9, "unexpected events: {events:?}");

        let find_event = |event_type: &str, distinct_id: Option<&str>| -> &Value {
            events
                .iter()
                .find(|event| {
                event["event"] == event_type
                        && distinct_id.map_or(true, |id| event["distinct_id"] == id)
                })
                .unwrap_or_else(|| {
                    panic!("missing event {event_type:?} {distinct_id:?}: {events:?}")
                })
        };

        let capture = find_event("e2e-capture", Some("user-1"));
        assert_eq!(capture["api_key"], "phc_e2e");
        assert_eq!(capture["properties"]["source"], "direct");

        let identify_direct = events
            .iter()
            .find(|event| {
                event["event"] == "$identify"
                    && event["person_properties"]["email"] == "user1@example.com"
            })
            .expect("missing direct identify event");
        assert!(identify_direct.get("api_key").is_none());

        let group_event = find_event("$groupidentify", Some("team-42"));
        assert_eq!(group_event["extra"]["group_type"], "team");
        assert_eq!(group_event["properties"]["members"], 3);

        let alias_direct = events
            .iter()
            .find(|event| {
                event["event"] == "$create_alias" && event["extra"]["alias"] == "user-1-alias"
            })
            .expect("missing direct alias event");
        assert!(alias_direct.get("api_key").is_none());

        let engage_event = find_event("$engage", Some("user-1"));
        assert_eq!(engage_event["extra"]["$set"]["plan"], "pro");
        assert_eq!(
            engage_event["extra"]["$unset"]
                .as_array()
                .unwrap()
                .as_slice(),
            [Value::String("temp".to_string())]
        );

        let batch_capture = find_event("e2e-batch-capture", Some("user-2"));
        assert_eq!(batch_capture["api_key"], "phc_batch");
        assert_eq!(batch_capture["properties"]["batch"], true);

        let batch_alias = events
            .iter()
            .find(|event| {
                event["event"] == "$create_alias"
                    && event["distinct_id"] == "user-2"
                    && event["extra"]["alias"] == "user-2-alias"
            })
            .expect("missing batch alias event");
        assert_eq!(batch_alias["api_key"], "phc_batch");

        let batch_identify = events
            .iter()
            .find(|event| {
                event["event"] == "$identify"
                    && event["distinct_id"] == "user-2"
                    && event["person_properties"]["email"] == "batch@example.com"
            })
            .expect("missing batch identify event");
        assert_eq!(batch_identify["api_key"], "phc_batch");

        let snapshot = find_event("$snapshot", Some("user-1"));
        assert_eq!(snapshot["api_key"], "phc_session");
        assert_eq!(
            snapshot["properties"]["data"]["metadata"]["distinct_id"],
            "user-1"
        );

        server_handle.abort();
        let _ = server_handle.await;
        Ok(()) as Result<(), Box<dyn std::error::Error>>
    }
    .await;

    stop_docker_pipeline().await.ok();
    test_result
}
