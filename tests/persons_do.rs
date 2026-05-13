#[path = "helpers/mod.rs"]
mod helpers;

use helpers::spawn_pipeline_stub;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::{json, Value};
use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tempfile::TempDir;
use tokio::process::{Child, Command};

#[derive(Debug, Deserialize)]
struct PersonDebugResponse {
    canonical_id: String,
    record: Option<Value>,
}

struct WranglerDev {
    child: Child,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
}

impl WranglerDev {
    fn logs(&self) -> String {
        let stdout = read_log(&self.stdout_path);
        let stderr = read_log(&self.stderr_path);
        format!("wrangler stdout:\n{stdout}\nwrangler stderr:\n{stderr}")
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_object_person_updates_apply() -> Result<(), Box<dyn std::error::Error>> {
    let (pipeline_endpoint, _pipeline_rx, pipeline_handle) = spawn_pipeline_stub().await?;

    let port = reserve_port()?;
    let temp_dir = TempDir::new()?;
    let debug_token = "debug-test-token";

    let config_path =
        write_wrangler_config(temp_dir.path(), &pipeline_endpoint.to_string(), debug_token)?;
    patch_worker_bundle()?;

    let mut wrangler = spawn_wrangler_dev(&config_path, port, temp_dir.path())?;
    wait_for_health(port, &mut wrangler).await?;

    let client = Client::builder().timeout(Duration::from_secs(5)).build()?;
    let base_url = format!("http://127.0.0.1:{port}");

    // identify with $set and $set_once
    client
        .post(format!("{base_url}/identify"))
        .json(&json!({
            "distinct_id": "person-1",
            "properties": {
                "$set": { "email": "person1@example.com" },
                "$set_once": { "created_at": "2024-01-01" }
            }
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
            "distinct_id": "person-1",
            "alias": "anon-1"
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
    dir: &Path,
    pipeline_endpoint: &str,
    debug_token: &str,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let main_path = std::env::current_dir()?.join("build/worker.mjs");
    let config = format!(
        r#"
name = "hogflare-test"
main = "{main}"
compatibility_date = "2025-01-09"

[vars]
CLOUDFLARE_PIPELINE_ENDPOINT = "{pipeline}"
CLOUDFLARE_PIPELINE_TIMEOUT_SECS = "5"
PERSON_DEBUG_TOKEN = "{debug_token}"

[build.upload]
format = "modules"

[[durable_objects.bindings]]
name = "PERSONS"
class_name = "PersonDurableObject"

[[durable_objects.bindings]]
name = "PERSON_ID_COUNTER"
class_name = "PersonIdCounterDurableObject"

[[migrations]]
tag = "v1"
new_sqlite_classes = ["PersonDurableObject", "PersonIdCounterDurableObject"]
"#,
        main = main_path.display(),
        pipeline = pipeline_endpoint,
        debug_token = debug_token
    );

    let path = dir.join("wrangler.toml");
    fs::write(&path, config.trim_start())?;
    Ok(path)
}

fn patch_worker_bundle() -> Result<(), Box<dyn std::error::Error>> {
    let build_dir = std::env::current_dir()?.join("build");
    let bundle_path = build_dir.join("index.js");
    if !bundle_path.exists() {
        return Err("missing build/index.js; run worker-build before tests".into());
    }
    let contents = fs::read_to_string(&bundle_path)?;
    let patched = if contents.starts_with("import source wasmModule") {
        contents.replacen("import source wasmModule from", "import wasmModule from", 1)
    } else {
        contents
    };

    fs::write(&bundle_path, &patched)?;
    fs::write(build_dir.join("index.mjs"), &patched)?;

    let worker_shim = r#"export { default } from "./index.mjs";
export * from "./index.mjs";
"#;
    fs::write(build_dir.join("worker.mjs"), worker_shim)?;
    Ok(())
}

fn spawn_wrangler_dev(
    config_path: &PathBuf,
    port: u16,
    log_dir: &Path,
) -> Result<WranglerDev, Box<dyn std::error::Error>> {
    let stdout_path = log_dir.join("wrangler.stdout.log");
    let stderr_path = log_dir.join("wrangler.stderr.log");
    let stdout = fs::File::create(&stdout_path)?;
    let stderr = fs::File::create(&stderr_path)?;

    let child = Command::new("bunx")
        .arg("wrangler")
        .arg("dev")
        .arg("--local")
        .arg("--config")
        .arg(config_path)
        .arg("--ip")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(port.to_string())
        .arg("--log-level")
        .arg("error")
        .env("WRANGLER_SEND_METRICS", "false")
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()?;

    Ok(WranglerDev {
        child,
        stdout_path,
        stderr_path,
    })
}

async fn wait_for_health(
    port: u16,
    wrangler: &mut WranglerDev,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder().timeout(Duration::from_secs(2)).build()?;
    let url = format!("http://127.0.0.1:{port}/healthz");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    let mut last_failure = String::from("no response yet");

    while tokio::time::Instant::now() < deadline {
        if let Some(status) = wrangler.child.try_wait()? {
            return Err(format!(
                "wrangler dev exited before health check succeeded: {status}\n{}",
                wrangler.logs()
            )
            .into());
        }

        match client.get(&url).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    return Ok(());
                }
                last_failure = format!("last status {}", resp.status());
            }
            Err(err) => {
                last_failure = err.to_string();
            }
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    if let Some(status) = wrangler.child.try_wait()? {
        return Err(format!(
            "wrangler dev exited before health check succeeded: {status}\n{}",
            wrangler.logs()
        )
        .into());
    }

    Err(format!(
        "timed out after 90s waiting for wrangler dev at {url}: {last_failure}\n{}",
        wrangler.logs()
    )
    .into())
}

fn read_log(path: &Path) -> String {
    match fs::read_to_string(path) {
        Ok(contents) if contents.trim().is_empty() => "<empty>".to_string(),
        Ok(contents) => contents,
        Err(err) => format!("<failed to read {}: {err}>", path.display()),
    }
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

async fn cleanup_pipeline(pipeline_handle: tokio::task::JoinHandle<()>) {
    pipeline_handle.abort();
    let _ = pipeline_handle.await;
}

async fn shutdown_wrangler(wrangler: &mut WranglerDev) {
    let _ = wrangler.child.kill().await;
}

async fn cleanup_wrangler(wrangler: &mut WranglerDev) {
    let _ = wrangler.child.wait().await;
}
