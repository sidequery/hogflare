#[path = "helpers/mod.rs"]
mod helpers;

use helpers::spawn_pipeline_stub;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::{json, Value};
use std::fs;
use std::net::TcpListener;
use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;
use tokio::process::{Child, Command};

#[derive(Debug, Deserialize)]
struct PersonDebugResponse {
    canonical_id: String,
    record: Option<Value>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_object_person_updates_apply() -> Result<(), Box<dyn std::error::Error>> {
    let (pipeline_endpoint, _pipeline_rx, pipeline_handle) = spawn_pipeline_stub().await?;

    let port = reserve_port()?;
    let temp_dir = TempDir::new()?;
    let debug_token = "debug-test-token";

    let config_path = write_wrangler_config(
        temp_dir.path(),
        &pipeline_endpoint.to_string(),
        debug_token,
    )?;

    let mut wrangler = spawn_wrangler_dev(&config_path, port)?;
    wait_for_health(port).await?;

    let client = Client::builder().timeout(Duration::from_secs(5)).build()?;
    let base_url = format!("http://127.0.0.1:{port}");

    // identify with $set and $set_once
    client
        .post(format!("{base_url}/identify"))
        .json(&json!({
            "distinct_id": "person-1",
            "properties": { "email": "person1@example.com" },
            "$set_once": { "created_at": "2024-01-01" }
        }))
        .send()
        .await?
        .error_for_status()?;

    let snapshot = fetch_person(&client, &base_url, debug_token, "person-1").await?;
    assert_eq!(snapshot.canonical_id, "person-1");
    let record = snapshot.record.expect("expected person record");
    assert_eq!(record["properties"]["email"], "person1@example.com");
    assert_eq!(record["properties_set_once"]["created_at"], "2024-01-01");

    // capture with $set
    client
        .post(format!("{base_url}/capture"))
        .json(&json!({
            "event": "plan-upgrade",
            "distinct_id": "person-1",
            "properties": {
                "$set": { "plan": "pro" }
            }
        }))
        .send()
        .await?
        .error_for_status()?;

    let snapshot = fetch_person(&client, &base_url, debug_token, "person-1").await?;
    let record = snapshot.record.expect("expected person record");
    assert_eq!(record["properties"]["plan"], "pro");

    // alias anon -> person-1
    client
        .post(format!("{base_url}/alias"))
        .json(&json!({
            "distinct_id": "anon-1",
            "alias": "person-1"
        }))
        .send()
        .await?
        .error_for_status()?;

    let alias_snapshot = fetch_person(&client, &base_url, debug_token, "anon-1").await?;
    assert_eq!(alias_snapshot.canonical_id, "person-1");

    shutdown_wrangler(&mut wrangler).await;
    cleanup_wrangler(&mut wrangler).await;
    cleanup_pipeline(pipeline_handle).await;
    Ok(())
}

fn reserve_port() -> Result<u16, Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    Ok(port)
}

fn write_wrangler_config(
    dir: &std::path::Path,
    pipeline_endpoint: &str,
    debug_token: &str,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let main_path = std::env::current_dir()?.join("build/index.js");
    let config = format!(
        r#"
name = "hogflare-test"
main = "{main}"
compatibility_date = "2025-01-09"

[vars]
CLOUDFLARE_PIPELINE_ENDPOINT = "{pipeline}"
CLOUDFLARE_PIPELINE_TIMEOUT_SECS = "5"
PERSON_DEBUG_TOKEN = "{debug_token}"

[[durable_objects.bindings]]
name = "PERSONS"
class_name = "PersonDurableObject"

[[migrations]]
tag = "v1"
new_classes = ["PersonDurableObject"]
"#,
        main = main_path.display(),
        pipeline = pipeline_endpoint,
        debug_token = debug_token
    );

    let path = dir.join("wrangler.toml");
    fs::write(&path, config.trim_start())?;
    Ok(path)
}

fn spawn_wrangler_dev(config_path: &PathBuf, port: u16) -> Result<Child, Box<dyn std::error::Error>> {
    let child = Command::new("bunx")
        .arg("wrangler")
        .arg("dev")
        .arg("--local")
        .arg("--no-bundle")
        .arg("--config")
        .arg(config_path)
        .arg("--ip")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(port.to_string())
        .arg("--log-level")
        .arg("error")
        .env("WRANGLER_SEND_METRICS", "false")
        .spawn()?;

    Ok(child)
}

async fn wait_for_health(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder().timeout(Duration::from_secs(1)).build()?;
    let url = format!("http://127.0.0.1:{port}/healthz");

    for _ in 0..60 {
        if let Ok(resp) = client.get(&url).send().await {
            if resp.status().is_success() {
                return Ok(());
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    Err("timed out waiting for wrangler dev".into())
}

async fn fetch_person(
    client: &Client,
    base_url: &str,
    token: &str,
    distinct_id: &str,
) -> Result<PersonDebugResponse, Box<dyn std::error::Error>> {
    let response = client
        .get(format!("{base_url}/__debug/person/{distinct_id}"))
        .header("x-hogflare-debug-token", token)
        .send()
        .await?;

    if response.status() == StatusCode::NOT_FOUND {
        return Err("debug endpoint not enabled".into());
    }

    let payload = response.json::<PersonDebugResponse>().await?;
    Ok(payload)
}

async fn cleanup_pipeline(
    pipeline_handle: tokio::task::JoinHandle<()>,
) {
    pipeline_handle.abort();
    let _ = pipeline_handle.await;
}

async fn shutdown_wrangler(child: &mut Child) {
    let _ = child.kill().await;
}

async fn cleanup_wrangler(child: &mut Child) {
    let _ = child.wait().await;
}
