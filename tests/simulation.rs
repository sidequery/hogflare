#[path = "helpers/mod.rs"]
mod helpers;

use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use helpers::{cleanup, spawn_pipeline_stub};
use hogflare::{
    feature_flags::FeatureFlagStore,
    groups::{GroupError, GroupRecord, GroupSnapshot, GroupStore, GroupTypeMap, GroupUpdate},
    pipeline::PipelineClient,
};
use reqwest::{Client, Url};
use serde_json::{json, Value};
use tokio::{net::TcpListener, sync::Mutex, task::JoinHandle};

const API_KEY: &str = "phc_simulation";
const DEBUG_TOKEN: &str = "debug-simulation-token";
const USER_COUNT: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UserPath {
    AnonOnly,
    DirectIdentified,
    AnonToIdentified,
    Batched,
}

#[derive(Debug, Clone)]
struct SimUser {
    index: usize,
    path: UserPath,
    anon_id: String,
    identified_id: String,
    canonical_id: String,
    plan: &'static str,
    company: String,
    enterprise_company: bool,
}

#[derive(Default)]
struct MemoryGroupStore {
    records: Mutex<HashMap<(String, String), GroupRecord>>,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl GroupStore for MemoryGroupStore {
    async fn apply_update(&self, update: GroupUpdate) -> Result<GroupSnapshot, GroupError> {
        let mut records = self.records.lock().await;
        let key = (update.group_type.clone(), update.group_key.clone());
        let record = records
            .entry(key)
            .or_insert_with(|| GroupRecord::new(update.group_type.clone(), update.group_key));
        record.apply_update(&update.properties);
        Ok(GroupSnapshot {
            record: Some(record.clone()),
        })
    }

    async fn get_snapshot(
        &self,
        group_type: &str,
        group_key: &str,
    ) -> Result<GroupSnapshot, GroupError> {
        let records = self.records.lock().await;
        Ok(GroupSnapshot {
            record: records
                .get(&(group_type.to_string(), group_key.to_string()))
                .cloned(),
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn simulates_mixed_people_flags_groups_and_sessions() -> Result<(), Box<dyn std::error::Error>>
{
    let (pipeline_endpoint, pipeline_rx, pipeline_handle) = spawn_pipeline_stub().await?;
    let collected_events = collect_pipeline_events(pipeline_rx);
    let flags = simulation_flags()?;
    let (address, server_handle) = spawn_simulation_app(pipeline_endpoint, flags).await?;
    let base_url = format!("http://{address}");
    let client = Client::builder().timeout(Duration::from_secs(5)).build()?;
    let users = simulation_users();

    seed_companies(&client, &base_url).await?;

    for user in &users {
        run_user_path(&client, &base_url, user).await?;

        if user.index % 10 == 0 {
            engage_user(&client, &base_url, user).await?;
        }

        if user.index % 20 == 0 {
            alias_user(&client, &base_url, user).await?;
        }

        if user.index % 25 == 0 {
            send_session_recording(&client, &base_url, user).await?;
        }

        assert_feature_flags(&client, &base_url, user).await?;
    }

    assert_flag_endpoint_config(&client, &base_url, &users[0]).await?;
    assert_disable_flags(&client, &base_url, &users[0]).await?;

    let expected_events = 5 + 225 + 10 + 5 + 4;
    let events = wait_for_collected_events(&collected_events, expected_events).await?;
    assert_eq!(events.len(), expected_events);

    assert_eq!(count_event(&events, "$groupidentify"), 5);
    assert_eq!(count_event(&events, "sim-anon-capture"), 50);
    assert_eq!(count_event(&events, "sim-identified-capture"), 25);
    assert_eq!(count_event(&events, "sim-post-identify-capture"), 25);
    assert_eq!(count_event(&events, "sim-batch-capture"), 25);
    assert_eq!(count_event(&events, "$identify"), 75);
    assert_eq!(count_event(&events, "$create_alias"), 30);
    assert_eq!(count_event(&events, "$engage"), 10);
    assert_eq!(count_event(&events, "$snapshot"), 4);

    assert_user_event_shape(&events, &users)?;
    assert_debug_people(&client, &base_url, &events, &users).await?;

    cleanup(server_handle, pipeline_handle).await;
    Ok(())
}

fn simulation_flags() -> Result<FeatureFlagStore, serde_json::Error> {
    FeatureFlagStore::from_json(
        &json!({
            "flags": [
                {
                    "key": "pro-plan",
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
                },
                {
                    "key": "enterprise-company",
                    "type": "boolean",
                    "active": true,
                    "rollout_percentage": 100,
                    "group_type": "company",
                    "conditions": [
                        {
                            "properties": [
                                {
                                    "key": "tier",
                                    "value": "enterprise",
                                    "type": "group",
                                    "group_type": "company"
                                }
                            ]
                        }
                    ],
                    "payload": { "scope": "company" }
                },
                {
                    "key": "checkout-copy",
                    "type": "multivariate",
                    "active": true,
                    "rollout_percentage": 100,
                    "variants": [
                        {
                            "key": "control",
                            "rollout_percentage": 50,
                            "payload": { "headline": "control" }
                        },
                        {
                            "key": "variant",
                            "rollout_percentage": 50,
                            "payload": { "headline": "variant" }
                        }
                    ]
                },
                {
                    "key": "prod-only",
                    "active": true,
                    "rollout_percentage": 100,
                    "evaluation_environments": ["prod"],
                    "payload": { "env": "prod" }
                }
            ]
        })
        .to_string(),
    )
}

fn simulation_users() -> Vec<SimUser> {
    (0..USER_COUNT)
        .map(|index| {
            let path = match index % 4 {
                0 => UserPath::AnonOnly,
                1 => UserPath::DirectIdentified,
                2 => UserPath::AnonToIdentified,
                _ => UserPath::Batched,
            };
            let company_index = index % 5;
            let company = format!("company-{company_index}");
            let plan = if index % 3 == 0 { "pro" } else { "free" };
            let anon_id = format!("sim-anon-{index:03}");
            let identified_id = format!("sim-user-{index:03}");
            let canonical_id = match path {
                UserPath::AnonOnly => anon_id.clone(),
                UserPath::DirectIdentified | UserPath::AnonToIdentified | UserPath::Batched => {
                    identified_id.clone()
                }
            };

            SimUser {
                index,
                path,
                anon_id,
                identified_id,
                canonical_id,
                plan,
                company,
                enterprise_company: company_index % 2 == 0,
            }
        })
        .collect()
}

async fn spawn_simulation_app(
    pipeline_endpoint: Url,
    feature_flags: FeatureFlagStore,
) -> Result<(SocketAddr, JoinHandle<()>), Box<dyn std::error::Error>> {
    let pipeline = PipelineClient::new(pipeline_endpoint, None, Duration::from_secs(5))?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let group_store: Arc<dyn GroupStore> = Arc::new(MemoryGroupStore::default());
    let group_type_map = GroupTypeMap::new([
        Some("company".to_string()),
        Some("project".to_string()),
        None,
        None,
        None,
    ]);

    let handle = tokio::spawn(async move {
        if let Err(err) = hogflare::serve_with_options(
            listener,
            Arc::new(pipeline),
            Some(42),
            group_store,
            group_type_map,
            Some(API_KEY.to_string()),
            Some("https://session.example.test".to_string()),
            None,
            Some(DEBUG_TOKEN.to_string()),
            Arc::new(feature_flags),
        )
        .await
        {
            eprintln!("hogflare simulation server terminated: {err}");
        }
    });

    Ok((address, handle))
}

fn collect_pipeline_events(
    mut pipeline_rx: tokio::sync::mpsc::Receiver<Vec<Value>>,
) -> Arc<Mutex<Vec<Value>>> {
    let collected = Arc::new(Mutex::new(Vec::new()));
    let target = Arc::clone(&collected);
    tokio::spawn(async move {
        while let Some(batch) = pipeline_rx.recv().await {
            target.lock().await.extend(batch);
        }
    });
    collected
}

async fn seed_companies(client: &Client, base_url: &str) -> Result<(), Box<dyn std::error::Error>> {
    for company_index in 0..5 {
        let tier = if company_index % 2 == 0 {
            "enterprise"
        } else {
            "startup"
        };
        post_ok(
            client,
            &format!("{base_url}/groups"),
            json!({
                "api_key": API_KEY,
                "group_type": "company",
                "group_key": format!("company-{company_index}"),
                "properties": {
                    "tier": tier,
                    "company_index": company_index,
                    "region": if company_index % 2 == 0 { "us" } else { "eu" }
                }
            }),
        )
        .await?;
    }

    Ok(())
}

async fn run_user_path(
    client: &Client,
    base_url: &str,
    user: &SimUser,
) -> Result<(), Box<dyn std::error::Error>> {
    match user.path {
        UserPath::AnonOnly => {
            post_ok(
                client,
                &format!("{base_url}/capture"),
                json!({
                    "api_key": API_KEY,
                    "event": "sim-anon-capture",
                    "distinct_id": user.anon_id,
                    "properties": capture_properties(user, "anon_only")
                }),
            )
            .await?;
        }
        UserPath::DirectIdentified => {
            post_ok(
                client,
                &format!("{base_url}/identify"),
                json!({
                    "api_key": API_KEY,
                    "distinct_id": user.identified_id,
                    "properties": identify_properties(user, "direct_identified")
                }),
            )
            .await?;
            post_ok(
                client,
                &format!("{base_url}/capture"),
                json!({
                    "api_key": API_KEY,
                    "event": "sim-identified-capture",
                    "distinct_id": user.identified_id,
                    "properties": capture_properties(user, "direct_identified")
                }),
            )
            .await?;
        }
        UserPath::AnonToIdentified => {
            post_ok(
                client,
                &format!("{base_url}/capture"),
                json!({
                    "api_key": API_KEY,
                    "event": "sim-anon-capture",
                    "distinct_id": user.anon_id,
                    "properties": capture_properties(user, "anon_to_identified")
                }),
            )
            .await?;
            post_ok(
                client,
                &format!("{base_url}/identify"),
                json!({
                    "api_key": API_KEY,
                    "distinct_id": user.identified_id,
                    "$anon_distinct_id": user.anon_id,
                    "properties": {
                        "$set": identify_properties(user, "anon_to_identified"),
                        "$set_once": {
                            "signup_source": "simulation"
                        }
                    }
                }),
            )
            .await?;
            post_ok(
                client,
                &format!("{base_url}/capture"),
                json!({
                    "api_key": API_KEY,
                    "event": "sim-post-identify-capture",
                    "distinct_id": user.identified_id,
                    "properties": capture_properties(user, "anon_to_identified")
                }),
            )
            .await?;
        }
        UserPath::Batched => {
            post_ok(
                client,
                &format!("{base_url}/batch"),
                json!({
                    "api_key": API_KEY,
                    "batch": [
                        {
                            "event": "sim-batch-capture",
                            "distinct_id": user.identified_id,
                            "properties": capture_properties(user, "batched")
                        },
                        {
                            "event": "$identify",
                            "distinct_id": user.identified_id,
                            "properties": identify_properties(user, "batched")
                        },
                        {
                            "type": "alias",
                            "distinct_id": user.identified_id,
                            "alias": format!("sim-batch-alias-{:03}", user.index)
                        }
                    ]
                }),
            )
            .await?;
        }
    }

    Ok(())
}

fn identify_properties(user: &SimUser, cohort: &str) -> Value {
    json!({
        "email": format!("user{:03}@example.com", user.index),
        "plan": user.plan,
        "cohort": cohort,
        "company": user.company
    })
}

fn capture_properties(user: &SimUser, cohort: &str) -> Value {
    json!({
        "cohort": cohort,
        "index": user.index,
        "plan": user.plan,
        "company": user.company,
        "$groups": {
            "company": user.company
        },
        "$group_set": {
            "company": {
                "last_user_index": user.index
            }
        },
        "$set": {
            "plan": user.plan,
            "cohort": cohort,
            "company": user.company
        },
        "$set_once": {
            "first_seen_source": "simulation"
        }
    })
}

async fn engage_user(
    client: &Client,
    base_url: &str,
    user: &SimUser,
) -> Result<(), Box<dyn std::error::Error>> {
    post_ok(
        client,
        &format!("{base_url}/engage"),
        json!({
            "api_key": API_KEY,
            "distinct_id": user.canonical_id,
            "$set": {
                "last_engaged_index": user.index
            },
            "$set_once": {
                "engaged_source": "simulation"
            },
            "$groups": {
                "company": user.company
            }
        }),
    )
    .await
}

async fn alias_user(
    client: &Client,
    base_url: &str,
    user: &SimUser,
) -> Result<(), Box<dyn std::error::Error>> {
    post_ok(
        client,
        &format!("{base_url}/alias"),
        json!({
            "api_key": API_KEY,
            "distinct_id": user.canonical_id,
            "alias": format!("sim-extra-alias-{:03}", user.index)
        }),
    )
    .await
}

async fn send_session_recording(
    client: &Client,
    base_url: &str,
    user: &SimUser,
) -> Result<(), Box<dyn std::error::Error>> {
    post_ok(
        client,
        &format!("{base_url}/s"),
        json!({
            "token": API_KEY,
            "data": {
                "metadata": {
                    "distinct_id": user.canonical_id
                },
                "snapshot_data": {
                    "type": "full_snapshot",
                    "source": "simulation"
                }
            }
        }),
    )
    .await
}

async fn assert_feature_flags(
    client: &Client,
    base_url: &str,
    user: &SimUser,
) -> Result<(), Box<dyn std::error::Error>> {
    let body = post_json(
        client,
        &format!("{base_url}/decide?v=2"),
        json!({
            "token": API_KEY,
            "distinct_id": user.canonical_id,
            "groups": {
                "company": user.company
            },
            "evaluation_environments": ["prod"]
        }),
    )
    .await?;
    let flags = body["featureFlags"]
        .as_object()
        .expect("decide should return featureFlags");
    let payloads = body["featureFlagPayloads"]
        .as_object()
        .expect("decide should return featureFlagPayloads");

    assert_eq!(
        flags.get("pro-plan"),
        Some(&Value::Bool(user.plan == "pro"))
    );
    assert_eq!(
        flags.get("enterprise-company"),
        Some(&Value::Bool(user.enterprise_company))
    );
    assert_eq!(flags.get("prod-only"), Some(&Value::Bool(true)));

    let variant = flags
        .get("checkout-copy")
        .and_then(Value::as_str)
        .expect("checkout-copy should return a variant");
    assert!(matches!(variant, "control" | "variant"));
    assert!(payloads.get("checkout-copy").is_some());

    if user.plan == "pro" {
        assert_eq!(payloads["pro-plan"]["tier"], "pro");
    } else {
        assert!(payloads.get("pro-plan").is_none());
    }

    Ok(())
}

async fn assert_flag_endpoint_config(
    client: &Client,
    base_url: &str,
    user: &SimUser,
) -> Result<(), Box<dyn std::error::Error>> {
    let body = post_json(
        client,
        &format!("{base_url}/flags?v=2&config=true"),
        json!({
            "token": API_KEY,
            "distinct_id": user.canonical_id,
            "groups": {
                "company": user.company
            },
            "evaluation_environments": ["prod"]
        }),
    )
    .await?;

    assert!(body["requestId"].as_str().is_some());
    assert!(body["evaluatedAt"].as_i64().is_some());
    assert_eq!(
        body["sessionRecording"]["endpoint"],
        "https://session.example.test"
    );
    assert_eq!(body["supportedCompression"][0], "gzip");
    assert!(body["flags"]["pro-plan"]["reason"]["code"]
        .as_str()
        .is_some());

    let dev_body = post_json(
        client,
        &format!("{base_url}/flags?v=2"),
        json!({
            "token": API_KEY,
            "distinct_id": user.canonical_id,
            "groups": {
                "company": user.company
            },
            "evaluation_environments": ["dev"]
        }),
    )
    .await?;
    assert!(dev_body["featureFlags"].get("prod-only").is_none());

    Ok(())
}

async fn assert_disable_flags(
    client: &Client,
    base_url: &str,
    user: &SimUser,
) -> Result<(), Box<dyn std::error::Error>> {
    let body = post_json(
        client,
        &format!("{base_url}/decide?v=2"),
        json!({
            "token": API_KEY,
            "distinct_id": user.canonical_id,
            "disable_flags": true
        }),
    )
    .await?;
    assert!(body["featureFlags"].as_object().unwrap().is_empty());
    assert!(body["featureFlagPayloads"].as_object().unwrap().is_empty());
    Ok(())
}

fn assert_user_event_shape(
    events: &[Value],
    users: &[SimUser],
) -> Result<(), Box<dyn std::error::Error>> {
    for user in users {
        let ids = [
            user.anon_id.as_str(),
            user.identified_id.as_str(),
            user.canonical_id.as_str(),
        ];
        let user_events: Vec<&Value> = events
            .iter()
            .filter(|event| {
                event
                    .get("distinct_id")
                    .and_then(Value::as_str)
                    .is_some_and(|distinct_id| ids.contains(&distinct_id))
            })
            .filter(|event| event["event"] != "$snapshot")
            .collect();

        assert!(!user_events.is_empty(), "missing events for {user:?}");
        let person_ids: HashSet<&str> = user_events
            .iter()
            .filter_map(|event| event.get("person_id").and_then(Value::as_str))
            .collect();
        assert_eq!(person_ids.len(), 1, "split person ids for {user:?}");

        for event in user_events {
            assert_eq!(event["team_id"], 42);
            assert_eq!(event["api_key"], API_KEY);
            assert!(event["person_created_at"].as_str().is_some());

            if event["event"] == "sim-anon-capture"
                || event["event"] == "sim-identified-capture"
                || event["event"] == "sim-post-identify-capture"
                || event["event"] == "sim-batch-capture"
            {
                assert_eq!(event["group0"], user.company);
                assert_eq!(
                    event["group_properties"]["company"]["tier"]
                        .as_str()
                        .is_some(),
                    true
                );
                assert_eq!(event["properties"]["plan"], user.plan);
                assert_eq!(event["person_properties"]["plan"], user.plan);
                assert_eq!(
                    event["person_properties"]["first_seen_source"],
                    "simulation"
                );
            }
        }
    }

    Ok(())
}

async fn assert_debug_people(
    client: &Client,
    base_url: &str,
    events: &[Value],
    users: &[SimUser],
) -> Result<(), Box<dyn std::error::Error>> {
    let anon_transition = users
        .iter()
        .find(|user| user.path == UserPath::AnonToIdentified)
        .expect("simulation should include anon-to-identified user");
    let anon_capture = find_event(events, "sim-anon-capture", &anon_transition.anon_id)
        .expect("missing anon transition capture");
    let identified_capture = find_event(
        events,
        "sim-post-identify-capture",
        &anon_transition.identified_id,
    )
    .expect("missing identified transition capture");
    assert_eq!(anon_capture["person_id"], identified_capture["person_id"]);

    let snapshot = debug_person(client, base_url, &anon_transition.anon_id).await?;
    assert_eq!(snapshot["canonical_id"], anon_transition.identified_id);
    assert_eq!(snapshot["record"]["uuid"], anon_capture["person_id"]);
    assert_eq!(
        snapshot["record"]["properties"]["plan"],
        anon_transition.plan
    );
    assert_eq!(
        snapshot["record"]["properties_set_once"]["signup_source"],
        "simulation"
    );

    let anon_only = users
        .iter()
        .find(|user| user.path == UserPath::AnonOnly)
        .expect("simulation should include anon-only user");
    let snapshot = debug_person(client, base_url, &anon_only.anon_id).await?;
    assert_eq!(snapshot["canonical_id"], anon_only.anon_id);
    assert_eq!(
        snapshot["record"]["distinct_ids"][0].as_str(),
        Some(anon_only.anon_id.as_str())
    );

    Ok(())
}

fn count_event(events: &[Value], event_name: &str) -> usize {
    events
        .iter()
        .filter(|event| event["event"] == event_name)
        .count()
}

fn find_event<'a>(events: &'a [Value], event_name: &str, distinct_id: &str) -> Option<&'a Value> {
    events
        .iter()
        .find(|event| event["event"] == event_name && event["distinct_id"] == distinct_id)
}

async fn wait_for_collected_events(
    events: &Arc<Mutex<Vec<Value>>>,
    expected_count: usize,
) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
    for _ in 0..100 {
        let snapshot = events.lock().await.clone();
        if snapshot.len() >= expected_count {
            return Ok(snapshot);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Err(format!(
        "timed out waiting for {expected_count} events, got {}",
        events.lock().await.len()
    )
    .into())
}

async fn debug_person(
    client: &Client,
    base_url: &str,
    distinct_id: &str,
) -> Result<Value, Box<dyn std::error::Error>> {
    let response = client
        .get(format!("{base_url}/__debug/person/{distinct_id}"))
        .header("x-hogflare-debug-token", DEBUG_TOKEN)
        .send()
        .await?;
    assert!(
        response.status().is_success(),
        "debug person failed: {}",
        response.status()
    );
    Ok(response.json().await?)
}

async fn post_ok(
    client: &Client,
    url: &str,
    payload: Value,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = client.post(url).json(&payload).send().await?;
    assert!(
        response.status().is_success(),
        "POST {url} failed with {}: {}",
        response.status(),
        response.text().await?
    );
    Ok(())
}

async fn post_json(
    client: &Client,
    url: &str,
    payload: Value,
) -> Result<Value, Box<dyn std::error::Error>> {
    let response = client.post(url).json(&payload).send().await?;
    assert!(
        response.status().is_success(),
        "POST {url} failed with {}: {}",
        response.status(),
        response.text().await?
    );
    Ok(response.json().await?)
}
