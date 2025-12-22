use std::collections::{HashMap, HashSet};

use serde::Deserialize;
use serde_json::{Map, Value};
use sha1::{Digest, Sha1};
use uuid::Uuid;

#[derive(Debug, Clone, Default)]
pub struct FeatureFlagEvaluationOptions {
    pub flag_keys: Option<HashSet<String>>,
    pub evaluation_environments: Option<HashSet<String>>,
}

#[derive(Debug, Clone, Default)]
pub struct FeatureFlagStore {
    flags: Vec<FeatureFlagDefinition>,
}

impl FeatureFlagStore {
    pub fn empty() -> Self {
        Self { flags: Vec::new() }
    }

    pub fn is_empty(&self) -> bool {
        self.flags.is_empty()
    }

    pub fn from_json(raw: &str) -> Result<Self, serde_json::Error> {
        let trimmed = raw.trim();
        if trimmed.starts_with('[') {
            let flags: Vec<FeatureFlagDefinition> = serde_json::from_str(trimmed)?;
            return Ok(Self { flags });
        }

        let parsed: FeatureFlagConfig = serde_json::from_str(trimmed)?;
        Ok(Self { flags: parsed.flags })
    }

    pub fn evaluate(&self, ctx: &FeatureFlagContext) -> FeatureFlagEvaluation {
        self.evaluate_with(ctx, &FeatureFlagEvaluationOptions::default())
    }

    pub fn evaluate_with(
        &self,
        ctx: &FeatureFlagContext,
        options: &FeatureFlagEvaluationOptions,
    ) -> FeatureFlagEvaluation {
        let mut results = Vec::new();
        for flag in &self.flags {
            if let Some(keys) = options.flag_keys.as_ref() {
                if !keys.contains(&flag.key) {
                    continue;
                }
            }
            if let Some(envs) = options.evaluation_environments.as_ref() {
                if !flag_matches_environment(flag, envs) {
                    continue;
                }
            }
            let outcome = evaluate_flag(flag, ctx);
            results.push(outcome);
        }
        FeatureFlagEvaluation {
            results,
            request_id: Uuid::new_v4().to_string(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct FeatureFlagConfig {
    #[serde(default)]
    flags: Vec<FeatureFlagDefinition>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FeatureFlagDefinition {
    #[serde(default)]
    pub id: Option<i64>,
    #[serde(default)]
    pub version: Option<i64>,
    #[serde(default)]
    pub description: Option<String>,
    pub key: String,
    #[serde(default = "default_true")]
    pub active: bool,
    #[serde(default, rename = "type")]
    pub flag_type: FeatureFlagType,
    #[serde(default)]
    pub rollout_percentage: Option<f64>,
    #[serde(default)]
    pub variants: Vec<FeatureFlagVariant>,
    #[serde(default)]
    pub payload: Option<Value>,
    #[serde(default)]
    pub variant_payloads: HashMap<String, Value>,
    #[serde(default)]
    pub conditions: Vec<FeatureFlagCondition>,
    #[serde(default)]
    pub group_type: Option<String>,
    #[serde(default)]
    pub evaluation_environments: Option<Vec<String>>,
    #[serde(default)]
    pub salt: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FeatureFlagType {
    #[default]
    Boolean,
    Multivariate,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FeatureFlagVariant {
    pub key: String,
    #[serde(default)]
    pub rollout_percentage: f64,
    #[serde(default)]
    pub payload: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FeatureFlagCondition {
    #[serde(default)]
    pub properties: Vec<PropertyFilter>,
    #[serde(default)]
    pub rollout_percentage: Option<f64>,
    #[serde(default)]
    pub variants: Vec<FeatureFlagVariant>,
    #[serde(default)]
    pub variant: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PropertyFilter {
    pub key: String,
    pub value: Value,
    #[serde(default, rename = "type")]
    pub property_type: Option<String>,
    #[serde(default)]
    pub group_type: Option<String>,
    #[serde(default, alias = "op", rename = "operator")]
    pub operator: Option<String>,
}

#[derive(Debug, Clone)]
pub struct FeatureFlagContext {
    pub distinct_id: String,
    pub person_properties: Map<String, Value>,
    pub groups: HashMap<String, String>,
    pub group_properties: HashMap<String, Map<String, Value>>,
}

#[derive(Debug, Clone)]
pub struct FeatureFlagEvaluation {
    results: Vec<FlagEvaluation>,
    request_id: String,
}

impl FeatureFlagEvaluation {
    pub fn empty() -> Self {
        Self {
            results: Vec::new(),
            request_id: Uuid::new_v4().to_string(),
        }
    }

    pub fn to_maps(&self, version: u8) -> (HashMap<String, Value>, HashMap<String, Value>) {
        let mut flags = HashMap::new();
        let mut payloads = HashMap::new();

        for result in &self.results {
            if version == 2 && !result.active {
                continue;
            }
            flags.insert(result.key.clone(), result.value.clone());
            if let Some(payload) = &result.payload {
                payloads.insert(result.key.clone(), payload.clone());
            }
        }

        (flags, payloads)
    }

    pub fn to_flag_details(&self, _version: u8) -> HashMap<String, Value> {
        let mut details = HashMap::new();

        for result in &self.results {
            details.insert(result.key.clone(), flag_detail(result));
        }

        details
    }

    pub fn is_empty(&self) -> bool {
        self.results.is_empty()
    }

    pub fn request_id(&self) -> String {
        self.request_id.clone()
    }
}

#[derive(Debug, Clone)]
struct FlagEvaluation {
    key: String,
    value: Value,
    payload: Option<Value>,
    active: bool,
    reason: Option<String>,
    condition_index: Option<usize>,
    flag_id: Option<i64>,
    flag_version: Option<i64>,
    flag_description: Option<String>,
}

fn evaluate_flag(flag: &FeatureFlagDefinition, ctx: &FeatureFlagContext) -> FlagEvaluation {
    if !flag.active {
        return build_evaluation(
            flag,
            Value::Bool(false),
            None,
            false,
            Some("disabled".to_string()),
            None,
        );
    }

    let active = true;
    let mut payloads = flag.variant_payloads.clone();
    for variant in &flag.variants {
        if let Some(payload) = &variant.payload {
            payloads.insert(variant.key.clone(), payload.clone());
        }
    }

    if !flag.conditions.is_empty() {
        for (index, condition) in flag.conditions.iter().enumerate() {
            if !properties_match(&condition.properties, ctx) {
                continue;
            }
            return evaluate_condition(flag, condition, ctx, &payloads, Some(index));
        }
        return build_evaluation(
            flag,
            Value::Bool(false),
            None,
            active,
            Some("no_match".to_string()),
            None,
        );
    }

    evaluate_condition(
        flag,
        &FeatureFlagCondition {
            properties: Vec::new(),
            rollout_percentage: flag.rollout_percentage,
            variants: flag.variants.clone(),
            variant: None,
        },
        ctx,
        &payloads,
        None,
    )
}

fn evaluate_condition(
    flag: &FeatureFlagDefinition,
    condition: &FeatureFlagCondition,
    ctx: &FeatureFlagContext,
    variant_payloads: &HashMap<String, Value>,
    condition_index: Option<usize>,
) -> FlagEvaluation {
    let active = flag.active;
    let hash_id = match resolve_hash_id(flag, ctx) {
        Some(id) => id,
        None => {
            return build_evaluation(
                flag,
                Value::Bool(false),
                None,
                active,
                Some("no_match".to_string()),
                condition_index,
            );
        }
    };

    let rollout = condition
        .rollout_percentage
        .or(flag.rollout_percentage)
        .unwrap_or(100.0);

    let salt = flag.salt.as_deref().unwrap_or(&flag.key);
    let bucket = bucket_for(salt, &hash_id);
    let allowed = bucket < rollout.max(0.0).min(100.0);

    if !allowed {
        return build_evaluation(
            flag,
            Value::Bool(false),
            None,
            active,
            Some("out_of_rollout".to_string()),
            condition_index,
        );
    }

    let variants = if !condition.variants.is_empty() {
        &condition.variants
    } else {
        &flag.variants
    };

    if flag.flag_type == FeatureFlagType::Multivariate || !variants.is_empty() {
        if let Some(variant) = &condition.variant {
            let payload = variant_payloads.get(variant).cloned();
            return build_evaluation(
                flag,
                Value::String(variant.clone()),
                payload,
                active,
                Some("match".to_string()),
                condition_index,
            );
        }

        if let Some(selected) = pick_variant(variants, salt, &hash_id) {
            let payload = variant_payloads.get(&selected).cloned();
            return build_evaluation(
                flag,
                Value::String(selected),
                payload,
                active,
                Some("match".to_string()),
                condition_index,
            );
        }
    }

    build_evaluation(
        flag,
        Value::Bool(true),
        flag.payload.clone(),
        active,
        Some("match".to_string()),
        condition_index,
    )
}

fn resolve_hash_id(flag: &FeatureFlagDefinition, ctx: &FeatureFlagContext) -> Option<String> {
    if let Some(group_type) = &flag.group_type {
        return ctx.groups.get(group_type).cloned();
    }
    Some(ctx.distinct_id.clone())
}

fn properties_match(filters: &[PropertyFilter], ctx: &FeatureFlagContext) -> bool {
    for filter in filters {
        if !property_matches(filter, ctx) {
            return false;
        }
    }

    true
}

fn pick_variant(
    variants: &[FeatureFlagVariant],
    salt: &str,
    hash_id: &str,
) -> Option<String> {
    if variants.is_empty() {
        return None;
    }

    let bucket = bucket_for(salt, hash_id);
    let mut cumulative = 0.0;
    for variant in variants {
        cumulative += variant.rollout_percentage.max(0.0);
        if bucket < cumulative.min(100.0) {
            return Some(variant.key.clone());
        }
    }

    None
}

fn bucket_for(salt: &str, hash_id: &str) -> f64 {
    let mut hasher = Sha1::new();
    hasher.update(salt.as_bytes());
    hasher.update(b":");
    hasher.update(hash_id.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    let value = u64::from_be_bytes(bytes);
    (value % 100) as f64
}

fn default_true() -> bool {
    true
}

fn flag_detail(result: &FlagEvaluation) -> Value {
    let enabled = match &result.value {
        Value::Bool(value) => *value,
        Value::String(_) => true,
        _ => true,
    };

    let mut detail = Map::new();
    detail.insert("key".to_string(), Value::String(result.key.clone()));
    detail.insert("enabled".to_string(), Value::Bool(enabled));

    if let Value::String(variant) = &result.value {
        detail.insert("variant".to_string(), Value::String(variant.clone()));
    }

    let mut metadata = Map::new();
    if let Some(id) = result.flag_id {
        metadata.insert("id".to_string(), Value::Number(id.into()));
    }
    if let Some(version) = result.flag_version {
        metadata.insert("version".to_string(), Value::Number(version.into()));
    }
    if let Some(description) = &result.flag_description {
        metadata.insert("description".to_string(), Value::String(description.clone()));
    }
    if let Some(payload) = &result.payload {
        let payload_str = serde_json::to_string(payload).unwrap_or_else(|_| "null".to_string());
        metadata.insert("payload".to_string(), Value::String(payload_str));
    }
    if !metadata.is_empty() {
        detail.insert("metadata".to_string(), Value::Object(metadata));
    }

    if let Some(reason) = &result.reason {
        let mut reason_obj = Map::new();
        reason_obj.insert("code".to_string(), Value::String(reason.clone()));
        if let Some(index) = result.condition_index {
            reason_obj.insert("condition_index".to_string(), Value::Number(index.into()));
        }
        detail.insert("reason".to_string(), Value::Object(reason_obj));
    }

    Value::Object(detail)
}

fn build_evaluation(
    flag: &FeatureFlagDefinition,
    value: Value,
    payload: Option<Value>,
    active: bool,
    reason: Option<String>,
    condition_index: Option<usize>,
) -> FlagEvaluation {
    FlagEvaluation {
        key: flag.key.clone(),
        value,
        payload,
        active,
        reason,
        condition_index,
        flag_id: flag.id,
        flag_version: flag.version,
        flag_description: flag.description.clone(),
    }
}

fn flag_matches_environment(flag: &FeatureFlagDefinition, envs: &HashSet<String>) -> bool {
    match flag.evaluation_environments.as_ref() {
        Some(list) if !list.is_empty() => list.iter().any(|env| envs.contains(env)),
        _ => true,
    }
}

fn property_matches(filter: &PropertyFilter, ctx: &FeatureFlagContext) -> bool {
    let property_type = filter.property_type.as_deref().unwrap_or("person");
    let operator = filter.operator.as_deref().unwrap_or("eq");
    let actual = match property_type {
        "group" => {
            let Some(group_type) = filter.group_type.as_ref() else {
                return false;
            };
            let Some(group_props) = ctx.group_properties.get(group_type) else {
                return false;
            };
            group_props.get(&filter.key)
        }
        _ => ctx.person_properties.get(&filter.key),
    };

    match operator {
        "is_set" => matches!(actual, Some(value) if !value.is_null()),
        "is_not" => match actual {
            Some(value) => !values_equal(value, &filter.value),
            None => false,
        },
        "in" => match actual {
            Some(value) => values_in(value, &filter.value, true),
            None => false,
        },
        "not_in" => match actual {
            Some(value) => values_in(value, &filter.value, false),
            None => false,
        },
        "contains" => match actual {
            Some(value) => value_contains(value, &filter.value),
            None => false,
        },
        "regex" => match actual {
            Some(value) => value_regex(value, &filter.value),
            None => false,
        },
        "gt" | "gte" | "lt" | "lte" => match actual {
            Some(value) => compare_numbers(value, &filter.value, operator),
            None => false,
        },
        _ => match actual {
            Some(value) => values_equal(value, &filter.value),
            None => false,
        },
    }
}

fn values_equal(actual: &Value, expected: &Value) -> bool {
    if actual == expected {
        return true;
    }

    if let (Some(actual_num), Some(expected_num)) = (coerce_number(actual), coerce_number(expected)) {
        return (actual_num - expected_num).abs() < f64::EPSILON;
    }

    if let (Some(actual_bool), Some(expected_bool)) = (coerce_bool(actual), coerce_bool(expected)) {
        return actual_bool == expected_bool;
    }

    false
}

fn values_in(actual: &Value, expected: &Value, positive: bool) -> bool {
    let Some(list) = expected.as_array() else {
        return false;
    };
    let found = list.iter().any(|item| values_equal(actual, item));
    if positive {
        found
    } else {
        !found
    }
}

fn value_contains(actual: &Value, expected: &Value) -> bool {
    match (actual, expected) {
        (Value::String(actual_str), Value::String(expected_str)) => {
            actual_str.contains(expected_str)
        }
        (Value::Array(actual_list), _) => actual_list.iter().any(|item| values_equal(item, expected)),
        _ => false,
    }
}

fn value_regex(actual: &Value, expected: &Value) -> bool {
    let Some(actual_str) = actual.as_str() else {
        return false;
    };
    let Some(pattern) = expected.as_str() else {
        return false;
    };
    let Ok(re) = regex::Regex::new(pattern) else {
        return false;
    };
    re.is_match(actual_str)
}

fn compare_numbers(actual: &Value, expected: &Value, operator: &str) -> bool {
    let (Some(actual_num), Some(expected_num)) = (coerce_number(actual), coerce_number(expected)) else {
        return false;
    };
    match operator {
        "gt" => actual_num > expected_num,
        "gte" => actual_num >= expected_num,
        "lt" => actual_num < expected_num,
        "lte" => actual_num <= expected_num,
        _ => false,
    }
}

fn coerce_number(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.trim().parse::<f64>().ok(),
        Value::Bool(value) => Some(if *value { 1.0 } else { 0.0 }),
        _ => None,
    }
}

fn coerce_bool(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(value) => Some(*value),
        Value::Number(number) => number.as_i64().map(|n| n != 0),
        Value::String(text) => match text.trim().to_lowercase().as_str() {
            "true" | "1" | "yes" => Some(true),
            "false" | "0" | "no" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn context_with_props(props: serde_json::Map<String, Value>) -> FeatureFlagContext {
        FeatureFlagContext {
            distinct_id: "user-1".to_string(),
            person_properties: props,
            groups: HashMap::new(),
            group_properties: HashMap::new(),
        }
    }

    #[test]
    fn filter_operators_and_coercion() {
        let flags = json!({
            "flags": [
                {
                    "key": "is_not",
                    "conditions": [
                        { "properties": [{ "key": "plan", "value": "free", "operator": "is_not" }] }
                    ]
                },
                {
                    "key": "in_list",
                    "conditions": [
                        { "properties": [{ "key": "plan", "value": ["pro", "enterprise"], "operator": "in" }] }
                    ]
                },
                {
                    "key": "contains",
                    "conditions": [
                        { "properties": [{ "key": "email", "value": "example.com", "operator": "contains" }] }
                    ]
                },
                {
                    "key": "regex",
                    "conditions": [
                        { "properties": [{ "key": "email", "value": ".*@example\\.com$", "operator": "regex" }] }
                    ]
                },
                {
                    "key": "is_set",
                    "conditions": [
                        { "properties": [{ "key": "plan", "value": true, "operator": "is_set" }] }
                    ]
                },
                {
                    "key": "gte_number",
                    "conditions": [
                        { "properties": [{ "key": "age", "value": 18, "operator": "gte" }] }
                    ]
                }
            ]
        })
        .to_string();

        let store = FeatureFlagStore::from_json(&flags).expect("valid flag json");
        let mut props = serde_json::Map::new();
        props.insert("plan".to_string(), Value::String("pro".to_string()));
        props.insert("email".to_string(), Value::String("test@example.com".to_string()));
        props.insert("age".to_string(), Value::String("21".to_string()));

        let ctx = context_with_props(props);
        let eval = store.evaluate(&ctx);
        let (values, _) = eval.to_maps(2);

        assert_eq!(values.get("is_not"), Some(&Value::Bool(true)));
        assert_eq!(values.get("in_list"), Some(&Value::Bool(true)));
        assert_eq!(values.get("contains"), Some(&Value::Bool(true)));
        assert_eq!(values.get("regex"), Some(&Value::Bool(true)));
        assert_eq!(values.get("is_set"), Some(&Value::Bool(true)));
        assert_eq!(values.get("gte_number"), Some(&Value::Bool(true)));
    }

    #[test]
    fn flags_respect_key_and_environment_filters() {
        let flags = json!({
            "flags": [
                { "key": "alpha" },
                { "key": "beta", "evaluation_environments": ["prod"] }
            ]
        })
        .to_string();

        let store = FeatureFlagStore::from_json(&flags).expect("valid flag json");
        let ctx = context_with_props(serde_json::Map::new());

        let mut options = FeatureFlagEvaluationOptions::default();
        options.flag_keys = Some(["alpha".to_string()].into_iter().collect());
        let eval = store.evaluate_with(&ctx, &options);
        let (values, _) = eval.to_maps(2);
        assert!(values.contains_key("alpha"));
        assert!(!values.contains_key("beta"));

        let mut options = FeatureFlagEvaluationOptions::default();
        options.evaluation_environments = Some(["dev".to_string()].into_iter().collect());
        let eval = store.evaluate_with(&ctx, &options);
        let (values, _) = eval.to_maps(2);
        assert!(values.contains_key("alpha"));
        assert!(!values.contains_key("beta"));
    }

    #[test]
    fn reasons_are_emitted() {
        let flags = json!({
            "flags": [
                { "key": "disabled", "active": false },
                {
                    "key": "no_match",
                    "conditions": [
                        { "properties": [{ "key": "plan", "value": "pro" }] }
                    ]
                },
                {
                    "key": "out_of_rollout",
                    "conditions": [
                        {
                            "rollout_percentage": 0,
                            "properties": [{ "key": "plan", "value": "free" }]
                        }
                    ]
                },
                {
                    "key": "match",
                    "conditions": [
                        { "properties": [{ "key": "plan", "value": "free" }] }
                    ]
                }
            ]
        })
        .to_string();

        let store = FeatureFlagStore::from_json(&flags).expect("valid flag json");
        let mut props = serde_json::Map::new();
        props.insert("plan".to_string(), Value::String("free".to_string()));
        let ctx = context_with_props(props);
        let details = store.evaluate(&ctx).to_flag_details(2);

        let reason = |key: &str| {
            details
                .get(key)
                .and_then(Value::as_object)
                .and_then(|obj| obj.get("reason"))
                .and_then(Value::as_object)
                .and_then(|obj| obj.get("code"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        };

        assert_eq!(reason("disabled"), "disabled");
        assert_eq!(reason("no_match"), "no_match");
        assert_eq!(reason("out_of_rollout"), "out_of_rollout");
        assert_eq!(reason("match"), "match");
    }
}
