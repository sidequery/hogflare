use std::collections::HashSet;

#[cfg(not(target_arch = "wasm32"))]
use std::collections::HashMap;

#[cfg(not(target_arch = "wasm32"))]
use std::sync::atomic::{AtomicI64, Ordering};

#[cfg(not(target_arch = "wasm32"))]
use tokio::sync::RwLock;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;
use uuid::Uuid;

use crate::models::{AliasRequest, CaptureRequest, EngageRequest, IdentifyRequest};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersonRecord {
    pub id: i64,
    pub team_id: Option<i64>,
    pub uuid: String,
    pub created_at: DateTime<Utc>,
    pub version: i64,
    pub distinct_ids: Vec<String>,
    pub properties: Map<String, Value>,
    pub properties_set_once: Map<String, Value>,
}

impl PersonRecord {
    pub fn new(distinct_id: String, team_id: Option<i64>, id: i64) -> Self {
        let mut record = Self {
            id,
            team_id,
            uuid: Uuid::new_v4().to_string(),
            created_at: Utc::now(),
            version: 0,
            distinct_ids: Vec::new(),
            properties: Map::new(),
            properties_set_once: Map::new(),
        };
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
        self.version = self.version.saturating_add(1);

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

        if secondary.created_at < merged.created_at {
            merged.created_at = secondary.created_at;
        }
        if merged.team_id.is_none() {
            merged.team_id = secondary.team_id;
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

        merged.version = merged.version.saturating_add(1);
        merged
    }

    pub fn merged_properties(&self) -> Value {
        let mut merged = self.properties.clone();
        for (key, value) in &self.properties_set_once {
            if !merged.contains_key(key) {
                merged.insert(key.clone(), value.clone());
            }
        }
        Value::Object(merged)
    }
}

impl Default for PersonRecord {
    fn default() -> Self {
        Self {
            id: 0,
            team_id: None,
            uuid: Uuid::new_v4().to_string(),
            created_at: Utc::now(),
            version: 0,
            distinct_ids: Vec::new(),
            properties: Map::new(),
            properties_set_once: Map::new(),
        }
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
    async fn apply_update(&self, update: PersonUpdate) -> Result<PersonSnapshot, PersonError>;
    async fn apply_alias(&self, alias: PersonAlias) -> Result<PersonSnapshot, PersonError>;
    async fn ensure_person(&self, distinct_id: &str) -> Result<PersonSnapshot, PersonError>;
    async fn get_snapshot(&self, distinct_id: &str) -> Result<PersonSnapshot, PersonError>;
}

pub struct NoopPersonStore;

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl PersonStore for NoopPersonStore {
    async fn apply_update(&self, update: PersonUpdate) -> Result<PersonSnapshot, PersonError> {
        Ok(PersonSnapshot {
            canonical_id: update.distinct_id,
            record: None,
        })
    }

    async fn apply_alias(&self, alias: PersonAlias) -> Result<PersonSnapshot, PersonError> {
        Ok(PersonSnapshot {
            canonical_id: alias.distinct_id,
            record: None,
        })
    }

    async fn ensure_person(&self, distinct_id: &str) -> Result<PersonSnapshot, PersonError> {
        Ok(PersonSnapshot {
            canonical_id: distinct_id.to_string(),
            record: None,
        })
    }

    async fn get_snapshot(&self, distinct_id: &str) -> Result<PersonSnapshot, PersonError> {
        Ok(PersonSnapshot {
            canonical_id: distinct_id.to_string(),
            record: None,
        })
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub struct MemoryPersonStore {
    records: RwLock<HashMap<String, PersonRecord>>,
    redirects: RwLock<HashMap<String, String>>,
    next_id: AtomicI64,
    team_id: Option<i64>,
}

#[cfg(not(target_arch = "wasm32"))]
impl MemoryPersonStore {
    pub fn new(team_id: Option<i64>) -> Self {
        Self {
            records: RwLock::new(HashMap::new()),
            redirects: RwLock::new(HashMap::new()),
            next_id: AtomicI64::new(1),
            team_id,
        }
    }

    async fn resolve_id(&self, distinct_id: &str) -> String {
        let redirects = self.redirects.read().await;
        let mut current = distinct_id.to_string();
        let mut hops = 0;
        while let Some(next) = redirects.get(&current) {
            current = next.clone();
            hops += 1;
            if hops > 10 {
                break;
            }
        }
        current
    }

    fn allocate_id(&self) -> i64 {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl PersonStore for MemoryPersonStore {
    async fn apply_update(&self, update: PersonUpdate) -> Result<PersonSnapshot, PersonError> {
        let canonical = self.resolve_id(&update.distinct_id).await;
        let mut records = self.records.write().await;
        let record = records
            .entry(canonical.clone())
            .or_insert_with(|| PersonRecord::new(canonical.clone(), self.team_id, 0));
        if record.id == 0 {
            record.id = self.allocate_id();
        }
        record.apply_update(&update);

        if canonical != update.distinct_id {
            let mut redirects = self.redirects.write().await;
            redirects.insert(update.distinct_id.clone(), canonical.clone());
        }

        Ok(PersonSnapshot {
            canonical_id: canonical,
            record: Some(record.clone()),
        })
    }

    async fn apply_alias(&self, alias: PersonAlias) -> Result<PersonSnapshot, PersonError> {
        let primary_id = self.resolve_id(&alias.distinct_id).await;
        let secondary_id = self.resolve_id(&alias.alias).await;

        if primary_id == secondary_id {
            let records = self.records.read().await;
            return Ok(PersonSnapshot {
                canonical_id: primary_id.clone(),
                record: records.get(&primary_id).cloned(),
            });
        }

        let mut records = self.records.write().await;
        let mut primary_record = records
            .get(&primary_id)
            .cloned()
            .unwrap_or_else(|| PersonRecord::new(primary_id.clone(), self.team_id, 0));
        if primary_record.id == 0 {
            primary_record.id = self.allocate_id();
        }
        primary_record.ensure_distinct_id(&alias.distinct_id);

        let mut secondary_record = records
            .get(&secondary_id)
            .cloned()
            .unwrap_or_else(|| PersonRecord::new(secondary_id.clone(), self.team_id, 0));
        if secondary_record.id == 0 {
            secondary_record.id = self.allocate_id();
        }
        secondary_record.ensure_distinct_id(&alias.alias);

        let merged = PersonRecord::merge(&primary_record, &secondary_record);
        records.insert(primary_id.clone(), merged.clone());

        let mut redirects = self.redirects.write().await;
        redirects.insert(secondary_id.clone(), primary_id.clone());
        redirects.insert(alias.alias.clone(), primary_id.clone());
        for id in &merged.distinct_ids {
            redirects.insert(id.clone(), primary_id.clone());
        }

        Ok(PersonSnapshot {
            canonical_id: primary_id,
            record: Some(merged),
        })
    }

    async fn ensure_person(&self, distinct_id: &str) -> Result<PersonSnapshot, PersonError> {
        let canonical = self.resolve_id(distinct_id).await;
        let mut records = self.records.write().await;
        let record = records
            .entry(canonical.clone())
            .or_insert_with(|| PersonRecord::new(canonical.clone(), self.team_id, 0));
        if record.id == 0 {
            record.id = self.allocate_id();
        }
        record.ensure_distinct_id(distinct_id);

        if canonical != distinct_id {
            let mut redirects = self.redirects.write().await;
            redirects.insert(distinct_id.to_string(), canonical.clone());
        }

        Ok(PersonSnapshot {
            canonical_id: canonical,
            record: Some(record.clone()),
        })
    }

    async fn get_snapshot(&self, distinct_id: &str) -> Result<PersonSnapshot, PersonError> {
        let canonical = self.resolve_id(distinct_id).await;
        let records = self.records.read().await;
        Ok(PersonSnapshot {
            canonical_id: canonical.clone(),
            record: records.get(&canonical).cloned(),
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
    let properties = request.properties.as_ref()?;
    let props = properties.as_object()?;
    let set = extract_object(props.get("$set"));
    let set_once = extract_object(props.get("$set_once"));

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
    const COUNTER_KEY: &str = "next_id";

    #[derive(Serialize, Deserialize)]
    struct ResolveResponse {
        redirect_to: Option<String>,
    }

    #[derive(Serialize, Deserialize)]
    struct RedirectRequest {
        redirect_to: String,
    }

    #[derive(Serialize, Deserialize)]
    struct CounterResponse {
        id: i64,
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
                        .unwrap_or_else(|| {
                            PersonRecord::new(update.distinct_id.clone(), None, 0)
                        });
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

    #[durable_object]
    pub struct PersonIdCounterDurableObject {
        state: State,
        _env: Env,
    }

    impl DurableObject for PersonIdCounterDurableObject {
        fn new(state: State, env: Env) -> Self {
            Self { state, _env: env }
        }

        async fn fetch(&self, req: Request) -> worker::Result<Response> {
            let path = req.path();
            let method = req.method();

            match (method, path.as_str()) {
                (Method::Post, "/next") => {
                    let current: Option<i64> = self.state.storage().get(COUNTER_KEY).await?;
                    let id = current.unwrap_or(1);
                    let next = id.saturating_add(1);
                    self.state.storage().put(COUNTER_KEY, next).await?;
                    Response::from_json(&CounterResponse { id })
                }
                _ => Response::error("not found", 404),
            }
        }
    }

    pub struct DurablePersonStore {
        namespace: ObjectNamespace,
        counter: Option<ObjectNamespace>,
        team_id: Option<i64>,
    }

    impl DurablePersonStore {
        pub fn new(
            namespace: ObjectNamespace,
            counter: Option<ObjectNamespace>,
            team_id: Option<i64>,
        ) -> Self {
            Self {
                namespace,
                counter,
                team_id,
            }
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

        async fn next_person_id(&self) -> Result<i64, PersonError> {
            let counter = self.counter.as_ref().ok_or_else(|| {
                PersonError::Message("missing PERSON_ID_COUNTER durable object binding".to_string())
            })?;
            let id = counter.id_from_name("global")?;
            let stub = id.get_stub()?;
            let mut init = RequestInit::new();
            init.with_method(Method::Post);
            let req = Request::new_with_init("https://person-counter.internal/next", &init)?;
            let mut response = stub.fetch_with_request(req).await?;
            let status = response.status_code();
            let body_text = response.text().await.unwrap_or_default();
            if !(200..300).contains(&status) {
                let message = format!(
                    "person id counter failed with status {status}: {body_text}"
                );
                worker::console_error!("{message}");
                return Err(PersonError::Message(message));
            }
            serde_json::from_str::<CounterResponse>(&body_text)
                .map_err(|err| {
                    let message = format!(
                        "person id counter returned invalid json: {err} ({body_text})"
                    );
                    worker::console_error!("{message}");
                    PersonError::Message(message)
                })
                .map(|resp| resp.id)
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
        async fn apply_update(&self, update: PersonUpdate) -> Result<PersonSnapshot, PersonError> {
            let canonical = self.resolve_id(&update.distinct_id).await?;
            let mut record = self
                .get_record(&canonical)
                .await?
                .unwrap_or_else(|| {
                    PersonRecord::new(
                        canonical.clone(),
                        self.team_id,
                        0,
                    )
                });
            if record.id == 0 {
                record.id = self.next_person_id().await?;
            }
            record.apply_update(&update);
            self.put_record(&canonical, record).await?;

            if canonical != update.distinct_id {
                self.redirect(&update.distinct_id, &canonical).await?;
            }

            let record = self.get_record(&canonical).await?;
            Ok(PersonSnapshot {
                canonical_id: canonical,
                record,
            })
        }

        async fn apply_alias(&self, alias: PersonAlias) -> Result<PersonSnapshot, PersonError> {
            let primary_id = self.resolve_id(&alias.distinct_id).await?;
            let secondary_id = self.resolve_id(&alias.alias).await?;

            if primary_id == secondary_id {
                let record = self.get_record(&primary_id).await?;
                return Ok(PersonSnapshot {
                    canonical_id: primary_id,
                    record,
                });
            }

            let mut primary_record = self
                .get_record(&primary_id)
                .await?
                .unwrap_or_else(|| PersonRecord::new(primary_id.clone(), self.team_id, 0));
            if primary_record.id == 0 {
                primary_record.id = self.next_person_id().await?;
            }
            primary_record.ensure_distinct_id(&alias.distinct_id);

            let mut secondary_record = self
                .get_record(&secondary_id)
                .await?
                .unwrap_or_else(|| PersonRecord::new(secondary_id.clone(), self.team_id, 0));
            if secondary_record.id == 0 {
                secondary_record.id = self.next_person_id().await?;
            }
            secondary_record.ensure_distinct_id(&alias.alias);

            let merged = PersonRecord::merge(&primary_record, &secondary_record);
            self.put_record(&primary_id, merged).await?;

            for id in &secondary_record.distinct_ids {
                self.redirect(id, &primary_id).await?;
            }
            self.redirect(&secondary_id, &primary_id).await?;
            self.redirect(&alias.alias, &primary_id).await?;

            let record = self.get_record(&primary_id).await?;
            Ok(PersonSnapshot {
                canonical_id: primary_id,
                record,
            })
        }

        async fn ensure_person(&self, distinct_id: &str) -> Result<PersonSnapshot, PersonError> {
            let canonical = self.resolve_id(distinct_id).await?;
            let mut record = self
                .get_record(&canonical)
                .await?
                .unwrap_or_else(|| PersonRecord::new(canonical.clone(), self.team_id, 0));
            if record.id == 0 {
                record.id = self.next_person_id().await?;
            }
            if !record.distinct_ids.iter().any(|id| id == distinct_id) {
                record.ensure_distinct_id(distinct_id);
                self.put_record(&canonical, record).await?;
            }

            if canonical != distinct_id {
                self.redirect(distinct_id, &canonical).await?;
            }

            let record = self.get_record(&canonical).await?;
            Ok(PersonSnapshot {
                canonical_id: canonical,
                record,
            })
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

    pub fn store_from_env(
        env: &Env,
        team_id: Option<i64>,
    ) -> std::sync::Arc<dyn PersonStore> {
        match env.durable_object("PERSONS") {
            Ok(namespace) => {
                let counter = env.durable_object("PERSON_ID_COUNTER").ok();
                std::sync::Arc::new(DurablePersonStore::new(namespace, counter, team_id))
            }
            Err(_) => std::sync::Arc::new(NoopPersonStore),
        }
    }
}

#[cfg(target_arch = "wasm32")]
pub use durable::store_from_env;

#[cfg(target_arch = "wasm32")]
pub use durable::PersonDurableObject;

#[cfg(target_arch = "wasm32")]
pub use durable::PersonIdCounterDurableObject;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn set_overwrites_properties() {
        let mut record = PersonRecord::new("user-1".to_string(), None, 1);
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
        let mut record = PersonRecord::new("user-1".to_string(), None, 1);
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
        let mut record = PersonRecord::new("user-1".to_string(), None, 1);
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
        let mut primary = PersonRecord::new("primary".to_string(), None, 1);
        primary
            .properties
            .insert("plan".to_string(), json!("pro"));
        primary
            .properties_set_once
            .insert("created_at".to_string(), json!("2024-01-01"));

        let mut secondary = PersonRecord::new("secondary".to_string(), None, 2);
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
