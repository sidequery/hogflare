use std::collections::HashSet;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;

use crate::models::{AliasRequest, CaptureRequest, EngageRequest, IdentifyRequest};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PersonRecord {
    pub distinct_ids: Vec<String>,
    pub properties: Map<String, Value>,
    pub properties_set_once: Map<String, Value>,
}

impl PersonRecord {
    pub fn new(distinct_id: String) -> Self {
        let mut record = Self::default();
        record.ensure_distinct_id(&distinct_id);
        record
    }

    pub fn ensure_distinct_id(&mut self, distinct_id: &str) {
        if !self.distinct_ids.iter().any(|id| id == distinct_id) {
            self.distinct_ids.push(distinct_id.to_string());
        }
    }

    pub fn apply_update(&mut self, update: &PersonUpdate) {
        self.ensure_distinct_id(&update.distinct_id);

        for (key, value) in &update.set {
            self.properties.insert(key.clone(), value.clone());
        }

        for (key, value) in &update.set_once {
            if self.properties.contains_key(key) || self.properties_set_once.contains_key(key) {
                continue;
            }
            self.properties_set_once.insert(key.clone(), value.clone());
        }

        for key in &update.unset {
            self.properties.remove(key);
            self.properties_set_once.remove(key);
        }
    }

    pub fn merge(primary: &PersonRecord, secondary: &PersonRecord) -> PersonRecord {
        let mut merged = primary.clone();
        let mut ids: HashSet<String> = merged.distinct_ids.iter().cloned().collect();

        for id in &secondary.distinct_ids {
            if ids.insert(id.clone()) {
                merged.distinct_ids.push(id.clone());
            }
        }

        for (key, value) in &secondary.properties {
            if !merged.properties.contains_key(key) {
                merged.properties.insert(key.clone(), value.clone());
            }
        }

        for (key, value) in &secondary.properties_set_once {
            if merged.properties.contains_key(key)
                || merged.properties_set_once.contains_key(key)
            {
                continue;
            }
            merged.properties_set_once.insert(key.clone(), value.clone());
        }

        merged
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PersonUpdate {
    pub distinct_id: String,
    pub set: Map<String, Value>,
    pub set_once: Map<String, Value>,
    pub unset: Vec<String>,
}

impl PersonUpdate {
    pub fn is_empty(&self) -> bool {
        self.set.is_empty() && self.set_once.is_empty() && self.unset.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct PersonAlias {
    pub distinct_id: String,
    pub alias: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersonSnapshot {
    pub canonical_id: String,
    pub record: Option<PersonRecord>,
}

#[derive(Debug, Error)]
pub enum PersonError {
    #[error("person update failed: {0}")]
    Message(String),
    #[error("failed to serialize person request: {0}")]
    Serialize(#[from] serde_json::Error),
    #[cfg(target_arch = "wasm32")]
    #[error("durable object request failed: {0}")]
    Worker(#[from] worker::Error),
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait PersonStore: Send + Sync {
    async fn apply_update(&self, update: PersonUpdate) -> Result<(), PersonError>;
    async fn apply_alias(&self, alias: PersonAlias) -> Result<(), PersonError>;
    async fn get_snapshot(&self, distinct_id: &str) -> Result<PersonSnapshot, PersonError>;
}

pub struct NoopPersonStore;

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl PersonStore for NoopPersonStore {
    async fn apply_update(&self, _update: PersonUpdate) -> Result<(), PersonError> {
        Ok(())
    }

    async fn apply_alias(&self, _alias: PersonAlias) -> Result<(), PersonError> {
        Ok(())
    }

    async fn get_snapshot(&self, distinct_id: &str) -> Result<PersonSnapshot, PersonError> {
        Ok(PersonSnapshot {
            canonical_id: distinct_id.to_string(),
            record: None,
        })
    }
}

pub fn update_from_capture(request: &CaptureRequest) -> Option<PersonUpdate> {
    let properties = request.properties.as_ref()?;
    let props = properties.as_object()?;
    let set = extract_object(props.get("$set"));
    let set_once = extract_object(props.get("$set_once"));
    let unset = extract_unset(props.get("$unset"));

    let update = PersonUpdate {
        distinct_id: request.distinct_id.clone(),
        set,
        set_once,
        unset,
    };

    if update.is_empty() {
        None
    } else {
        Some(update)
    }
}

pub fn update_from_identify(request: &IdentifyRequest) -> Option<PersonUpdate> {
    let set = extract_object(request.properties.as_ref());
    let set_once = extract_object(request.extra.get("$set_once"));

    let update = PersonUpdate {
        distinct_id: request.distinct_id.clone(),
        set,
        set_once,
        unset: Vec::new(),
    };

    if update.is_empty() {
        None
    } else {
        Some(update)
    }
}

pub fn update_from_engage(request: &EngageRequest) -> Option<PersonUpdate> {
    let set = extract_object(request.set.as_ref());
    let set_once = extract_object(request.set_once.as_ref());
    let unset = extract_unset(request.unset.as_ref());

    let update = PersonUpdate {
        distinct_id: request.distinct_id.clone(),
        set,
        set_once,
        unset,
    };

    if update.is_empty() {
        None
    } else {
        Some(update)
    }
}

pub fn alias_from_request(request: &AliasRequest) -> PersonAlias {
    PersonAlias {
        distinct_id: request.distinct_id.clone(),
        alias: request.alias.clone(),
    }
}

fn extract_object(value: Option<&Value>) -> Map<String, Value> {
    match value {
        Some(Value::Object(map)) => map.clone(),
        _ => Map::new(),
    }
}

fn extract_unset(value: Option<&Value>) -> Vec<String> {
    match value {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|item| item.as_str().map(|s| s.to_string()))
            .collect(),
        Some(Value::Object(map)) => map.keys().cloned().collect(),
        Some(Value::String(value)) => vec![value.clone()],
        _ => Vec::new(),
    }
}

#[cfg(target_arch = "wasm32")]
mod durable {
    use super::*;

    use worker::{
        durable_object, DurableObject, Env, Headers, Method, ObjectNamespace, Request, RequestInit,
        Response, State,
    };
    use worker::wasm_bindgen;
    use worker::wasm_bindgen::JsValue;

    const RECORD_KEY: &str = "record";
    const REDIRECT_KEY: &str = "redirect";

    #[derive(Serialize, Deserialize)]
    struct ResolveResponse {
        redirect_to: Option<String>,
    }

    #[derive(Serialize, Deserialize)]
    struct RedirectRequest {
        redirect_to: String,
    }

    #[durable_object]
    pub struct PersonDurableObject {
        state: State,
        _env: Env,
    }

    impl DurableObject for PersonDurableObject {
        fn new(state: State, env: Env) -> Self {
            Self { state, _env: env }
        }

        async fn fetch(&self, mut req: Request) -> worker::Result<Response> {
            let path = req.path();
            let method = req.method();

            match (method, path.as_str()) {
                (Method::Get, "/resolve") => {
                    let redirect: Option<String> = self.state.storage().get(REDIRECT_KEY).await?;
                    Response::from_json(&ResolveResponse { redirect_to: redirect })
                }
                (Method::Get, "/record") => {
                    let record: Option<PersonRecord> = self.state.storage().get(RECORD_KEY).await?;
                    Response::from_json(&record)
                }
                (Method::Post, "/apply") => {
                    let update: PersonUpdate = req.json().await?;
                    let mut record = self
                        .state
                        .storage()
                        .get::<PersonRecord>(RECORD_KEY)
                        .await?
                        .unwrap_or_else(|| PersonRecord::new(update.distinct_id.clone()));
                    record.apply_update(&update);
                    self.state.storage().put(RECORD_KEY, record).await?;
                    Response::from_json(&serde_json::json!({ "ok": true }))
                }
                (Method::Post, "/set_record") => {
                    let record: PersonRecord = req.json().await?;
                    self.state.storage().put(RECORD_KEY, record).await?;
                    Response::from_json(&serde_json::json!({ "ok": true }))
                }
                (Method::Post, "/redirect") => {
                    let body: RedirectRequest = req.json().await?;
                    self.state
                        .storage()
                        .put(REDIRECT_KEY, body.redirect_to)
                        .await?;
                    Response::from_json(&serde_json::json!({ "ok": true }))
                }
                _ => Response::error("not found", 404),
            }
        }
    }

    pub struct DurablePersonStore {
        namespace: ObjectNamespace,
    }

    impl DurablePersonStore {
        pub fn new(namespace: ObjectNamespace) -> Self {
            Self { namespace }
        }

        fn stub_for(&self, distinct_id: &str) -> Result<worker::durable::Stub, PersonError> {
            let id = self.namespace.id_from_name(distinct_id)?;
            Ok(id.get_stub()?)
        }

        async fn request_json<T: for<'de> Deserialize<'de>>(
            &self,
            distinct_id: &str,
            method: Method,
            path: &str,
            body: Option<&impl Serialize>,
        ) -> Result<T, PersonError> {
            let stub = self.stub_for(distinct_id)?;
            let mut init = RequestInit::new();
            init.with_method(method);

            let headers = Headers::new();
            headers
                .set("content-type", "application/json")
                .map_err(PersonError::Worker)?;
            init.with_headers(headers);

            if let Some(body) = body {
                let payload = serde_json::to_string(body)?;
                init.with_body(Some(JsValue::from_str(&payload)));
            }

            let req = Request::new_with_init(
                &format!("https://person.internal{path}"),
                &init,
            )?;
            let mut response = stub.fetch_with_request(req).await?;
            let status = response.status_code();
            let body_text = response.text().await.unwrap_or_default();
            if !(200..300).contains(&status) {
                let message = format!(
                    "durable object {path} failed with status {status}: {body_text}"
                );
                worker::console_error!("{message}");
                return Err(PersonError::Message(message));
            }

            serde_json::from_str(&body_text).map_err(|err| {
                let message = format!(
                    "durable object {path} returned invalid json: {err} ({body_text})"
                );
                worker::console_error!("{message}");
                PersonError::Message(message)
            })
        }

        async fn request_empty(
            &self,
            distinct_id: &str,
            method: Method,
            path: &str,
            body: Option<&impl Serialize>,
        ) -> Result<(), PersonError> {
            let _ = self
                .request_json::<serde_json::Value>(distinct_id, method, path, body)
                .await?;
            Ok(())
        }

        async fn resolve_id(&self, distinct_id: &str) -> Result<String, PersonError> {
            let response: ResolveResponse = self
                .request_json(distinct_id, Method::Get, "/resolve", None::<&()>)
                .await?;
            Ok(response.redirect_to.unwrap_or_else(|| distinct_id.to_string()))
        }

        async fn get_record(
            &self,
            distinct_id: &str,
        ) -> Result<Option<PersonRecord>, PersonError> {
            let record: Option<PersonRecord> = self
                .request_json(distinct_id, Method::Get, "/record", None::<&()>)
                .await?;
            Ok(record)
        }

        async fn put_record(
            &self,
            distinct_id: &str,
            record: PersonRecord,
        ) -> Result<(), PersonError> {
            self.request_empty(distinct_id, Method::Post, "/set_record", Some(&record))
                .await
        }

        async fn redirect(
            &self,
            distinct_id: &str,
            redirect_to: &str,
        ) -> Result<(), PersonError> {
            self.request_empty(
                distinct_id,
                Method::Post,
                "/redirect",
                Some(&RedirectRequest {
                    redirect_to: redirect_to.to_string(),
                }),
            )
            .await
        }
    }

    #[async_trait(?Send)]
    impl PersonStore for DurablePersonStore {
        async fn apply_update(&self, update: PersonUpdate) -> Result<(), PersonError> {
            let canonical = self.resolve_id(&update.distinct_id).await?;
            let mut record = self
                .get_record(&canonical)
                .await?
                .unwrap_or_else(|| PersonRecord::new(canonical.clone()));
            record.apply_update(&update);
            self.put_record(&canonical, record).await?;

            if canonical != update.distinct_id {
                self.redirect(&update.distinct_id, &canonical).await?;
            }

            Ok(())
        }

        async fn apply_alias(&self, alias: PersonAlias) -> Result<(), PersonError> {
            let from_id = self.resolve_id(&alias.distinct_id).await?;
            let to_id = self.resolve_id(&alias.alias).await?;

            if from_id == to_id {
                return Ok(());
            }

            let from_record = self
                .get_record(&from_id)
                .await?
                .unwrap_or_else(|| PersonRecord::new(from_id.clone()));
            let to_record = self
                .get_record(&to_id)
                .await?
                .unwrap_or_else(|| PersonRecord::new(to_id.clone()));

            let merged = PersonRecord::merge(&to_record, &from_record);
            self.put_record(&to_id, merged).await?;

            for id in &from_record.distinct_ids {
                self.redirect(id, &to_id).await?;
            }
            self.redirect(&from_id, &to_id).await?;
            self.redirect(&alias.distinct_id, &to_id).await?;

            Ok(())
        }

        async fn get_snapshot(&self, distinct_id: &str) -> Result<PersonSnapshot, PersonError> {
            let canonical = self.resolve_id(distinct_id).await?;
            let record = self.get_record(&canonical).await?;
            Ok(PersonSnapshot {
                canonical_id: canonical,
                record,
            })
        }
    }

    pub fn store_from_env(env: &Env) -> std::sync::Arc<dyn PersonStore> {
        match env.durable_object("PERSONS") {
            Ok(namespace) => std::sync::Arc::new(DurablePersonStore::new(namespace)),
            Err(_) => std::sync::Arc::new(NoopPersonStore),
        }
    }
}

#[cfg(target_arch = "wasm32")]
pub use durable::store_from_env;

#[cfg(target_arch = "wasm32")]
pub use durable::PersonDurableObject;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn set_overwrites_properties() {
        let mut record = PersonRecord::new("user-1".to_string());
        record.properties.insert("plan".to_string(), json!("basic"));

        let mut set = Map::new();
        set.insert("plan".to_string(), json!("pro"));
        let update = PersonUpdate {
            distinct_id: "user-1".to_string(),
            set,
            set_once: Map::new(),
            unset: Vec::new(),
        };

        record.apply_update(&update);
        assert_eq!(record.properties.get("plan"), Some(&json!("pro")));
    }

    #[test]
    fn set_once_ignores_existing() {
        let mut record = PersonRecord::new("user-1".to_string());
        record
            .properties
            .insert("created_at".to_string(), json!("2024-01-01"));

        let mut set_once = Map::new();
        set_once.insert("created_at".to_string(), json!("2025-01-01"));
        let update = PersonUpdate {
            distinct_id: "user-1".to_string(),
            set: Map::new(),
            set_once,
            unset: Vec::new(),
        };

        record.apply_update(&update);
        assert_eq!(
            record.properties.get("created_at"),
            Some(&json!("2024-01-01"))
        );
        assert!(record.properties_set_once.is_empty());
    }

    #[test]
    fn unset_removes_properties() {
        let mut record = PersonRecord::new("user-1".to_string());
        record.properties.insert("plan".to_string(), json!("pro"));
        record
            .properties_set_once
            .insert("created_at".to_string(), json!("2024-01-01"));

        let update = PersonUpdate {
            distinct_id: "user-1".to_string(),
            set: Map::new(),
            set_once: Map::new(),
            unset: vec!["plan".to_string(), "created_at".to_string()],
        };

        record.apply_update(&update);
        assert!(record.properties.is_empty());
        assert!(record.properties_set_once.is_empty());
    }

    #[test]
    fn merge_prefers_primary() {
        let mut primary = PersonRecord::new("primary".to_string());
        primary
            .properties
            .insert("plan".to_string(), json!("pro"));
        primary
            .properties_set_once
            .insert("created_at".to_string(), json!("2024-01-01"));

        let mut secondary = PersonRecord::new("secondary".to_string());
        secondary
            .properties
            .insert("plan".to_string(), json!("basic"));
        secondary
            .properties
            .insert("region".to_string(), json!("us"));
        secondary
            .properties_set_once
            .insert("created_at".to_string(), json!("2023-01-01"));

        let merged = PersonRecord::merge(&primary, &secondary);
        assert_eq!(merged.properties.get("plan"), Some(&json!("pro")));
        assert_eq!(merged.properties.get("region"), Some(&json!("us")));
        assert_eq!(
            merged.properties_set_once.get("created_at"),
            Some(&json!("2024-01-01"))
        );
        assert!(merged.distinct_ids.contains(&"primary".to_string()));
        assert!(merged.distinct_ids.contains(&"secondary".to_string()));
    }
}
