use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Map;
use thiserror::Error;

#[cfg(target_arch = "wasm32")]
use worker::{Env, Headers, Method, Request, RequestInit, Response, State};

#[cfg(target_arch = "wasm32")]
use worker::wasm_bindgen::JsValue;

#[cfg(target_arch = "wasm32")]
use worker::durable::{DurableObject, ObjectNamespace};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupRecord {
    pub group_type: String,
    pub group_key: String,
    pub created_at: DateTime<Utc>,
    pub version: i64,
    pub properties: Map<String, serde_json::Value>,
}

impl GroupRecord {
    pub fn new(group_type: String, group_key: String) -> Self {
        Self {
            group_type,
            group_key,
            created_at: Utc::now(),
            version: 0,
            properties: Map::new(),
        }
    }

    pub fn apply_update(&mut self, props: &Map<String, serde_json::Value>) {
        self.version = self.version.saturating_add(1);
        for (key, value) in props {
            self.properties.insert(key.clone(), value.clone());
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GroupSnapshot {
    pub record: Option<GroupRecord>,
}

#[derive(Debug, Clone)]
pub struct GroupUpdate {
    pub group_type: String,
    pub group_key: String,
    pub properties: Map<String, serde_json::Value>,
}

#[derive(Debug, Error)]
pub enum GroupError {
    #[error("group update failed: {0}")]
    Message(String),
    #[error("failed to serialize group request: {0}")]
    Serialize(#[from] serde_json::Error),
    #[cfg(target_arch = "wasm32")]
    #[error("durable object request failed: {0}")]
    Worker(#[from] worker::Error),
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait GroupStore: Send + Sync {
    async fn apply_update(&self, update: GroupUpdate) -> Result<GroupSnapshot, GroupError>;
    async fn get_snapshot(
        &self,
        group_type: &str,
        group_key: &str,
    ) -> Result<GroupSnapshot, GroupError>;
}

pub struct NoopGroupStore;

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl GroupStore for NoopGroupStore {
    async fn apply_update(&self, _update: GroupUpdate) -> Result<GroupSnapshot, GroupError> {
        Ok(GroupSnapshot { record: None })
    }

    async fn get_snapshot(
        &self,
        _group_type: &str,
        _group_key: &str,
    ) -> Result<GroupSnapshot, GroupError> {
        Ok(GroupSnapshot { record: None })
    }
}

#[derive(Debug, Clone)]
pub struct GroupTypeMap {
    types: [Option<String>; 5],
}

impl GroupTypeMap {
    pub fn new(types: [Option<String>; 5]) -> Self {
        Self { types }
    }

    pub fn index_for(&self, group_type: &str) -> Option<usize> {
        self.types
            .iter()
            .position(|value| value.as_deref() == Some(group_type))
    }

    pub fn types(&self) -> &[Option<String>; 5] {
        &self.types
    }
}

impl Default for GroupTypeMap {
    fn default() -> Self {
        Self {
            types: [None, None, None, None, None],
        }
    }
}

#[cfg(target_arch = "wasm32")]
mod durable {
    use super::*;
    use serde::Deserialize;

    const RECORD_KEY: &str = "record";

    #[durable_object]
    pub struct GroupDurableObject {
        state: State,
        _env: Env,
    }

    impl DurableObject for GroupDurableObject {
        fn new(state: State, env: Env) -> Self {
            Self { state, _env: env }
        }

        async fn fetch(&self, mut req: Request) -> worker::Result<Response> {
            let path = req.path();
            let method = req.method();

            match (method, path.as_str()) {
                (Method::Get, "/record") => {
                    let record: Option<GroupRecord> =
                        self.state.storage().get(RECORD_KEY).await?;
                    Response::from_json(&record)
                }
                (Method::Post, "/apply") => {
                    let update: GroupUpdate = req.json().await?;
                    let mut record = self
                        .state
                        .storage()
                        .get::<GroupRecord>(RECORD_KEY)
                        .await?
                        .unwrap_or_else(|| {
                            GroupRecord::new(update.group_type.clone(), update.group_key.clone())
                        });
                    record.apply_update(&update.properties);
                    self.state.storage().put(RECORD_KEY, record).await?;
                    Response::from_json(&serde_json::json!({ "ok": true }))
                }
                _ => Response::error("not found", 404),
            }
        }
    }

    pub struct DurableGroupStore {
        namespace: ObjectNamespace,
    }

    impl DurableGroupStore {
        pub fn new(namespace: ObjectNamespace) -> Self {
            Self { namespace }
        }

        fn stub_for(
            &self,
            group_type: &str,
            group_key: &str,
        ) -> Result<worker::durable::Stub, GroupError> {
            let name = format!("{group_type}:{group_key}");
            let id = self.namespace.id_from_name(&name)?;
            Ok(id.get_stub()?)
        }

        async fn request_json<T: for<'de> Deserialize<'de>>(
            &self,
            group_type: &str,
            group_key: &str,
            method: Method,
            path: &str,
            body: Option<&impl Serialize>,
        ) -> Result<T, GroupError> {
            let stub = self.stub_for(group_type, group_key)?;
            let mut init = RequestInit::new();
            init.with_method(method);

            let headers = Headers::new();
            headers
                .set("content-type", "application/json")
                .map_err(GroupError::Worker)?;
            init.with_headers(headers);

            if let Some(body) = body {
                let payload = serde_json::to_string(body)?;
                init.with_body(Some(JsValue::from_str(&payload)));
            }

            let req = Request::new_with_init(&format!("https://group.internal{path}"), &init)?;
            let mut response = stub.fetch_with_request(req).await?;
            let status = response.status_code();
            let body_text = response.text().await.unwrap_or_default();
            if !(200..300).contains(&status) {
                let message = format!(
                    "durable object {path} failed with status {status}: {body_text}"
                );
                worker::console_error!("{message}");
                return Err(GroupError::Message(message));
            }

            serde_json::from_str(&body_text).map_err(|err| {
                let message =
                    format!("durable object {path} returned invalid json: {err} ({body_text})");
                worker::console_error!("{message}");
                GroupError::Message(message)
            })
        }
    }

    #[async_trait(?Send)]
    impl GroupStore for DurableGroupStore {
        async fn apply_update(&self, update: GroupUpdate) -> Result<GroupSnapshot, GroupError> {
            self.request_json::<serde_json::Value>(
                &update.group_type,
                &update.group_key,
                Method::Post,
                "/apply",
                Some(&update),
            )
            .await?;
            let record = self
                .request_json(
                    &update.group_type,
                    &update.group_key,
                    Method::Get,
                    "/record",
                    None::<&()>,
                )
                .await?;
            Ok(GroupSnapshot { record })
        }

        async fn get_snapshot(
            &self,
            group_type: &str,
            group_key: &str,
        ) -> Result<GroupSnapshot, GroupError> {
            let record = self
                .request_json(group_type, group_key, Method::Get, "/record", None::<&()>)
                .await?;
            Ok(GroupSnapshot { record })
        }
    }

    pub fn store_from_env(env: &Env) -> std::sync::Arc<dyn GroupStore> {
        match env.durable_object("GROUPS") {
            Ok(namespace) => std::sync::Arc::new(DurableGroupStore::new(namespace)),
            Err(_) => std::sync::Arc::new(NoopGroupStore),
        }
    }
}

#[cfg(target_arch = "wasm32")]
pub use durable::store_from_env;

#[cfg(target_arch = "wasm32")]
pub use durable::GroupDurableObject;

