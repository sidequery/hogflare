use std::io::Read;

use axum::async_trait;
use axum::body::{to_bytes, Body};
use axum::extract::{FromRequest, FromRequestParts};
use axum::http::{header, request::Parts, HeaderMap, Request, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use chrono::{DateTime, Utc};
use flate2::read::{GzDecoder, ZlibDecoder};
use hex::encode as hex_encode;
use hmac::{Hmac, Mac};
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};
use serde_urlencoded;
use sha1::Sha1;
use sha2::Sha256;
use subtle::ConstantTimeEq;
use thiserror::Error;
use tracing::warn;

use crate::models::{
    AliasRequest, BatchRequest, CaptureRequest, EngageRequest, ErrorResponse, GroupIdentifyRequest,
    IdentifyRequest,
};
use crate::AppState;

#[cfg(target_arch = "wasm32")]
use worker::Cf;

use std::convert::Infallible;

pub struct PostHogPayload<T> {
    pub items: Vec<T>,
    pub sent_at: Option<DateTime<Utc>>,
}

pub struct PostHogBatchPayload {
    pub batch: BatchRequest,
}

pub struct RequestEnrichment {
    properties: Map<String, Value>,
}

impl RequestEnrichment {
    pub fn properties(&self) -> &Map<String, Value> {
        &self.properties
    }
}

#[async_trait]
impl<S> FromRequestParts<S> for RequestEnrichment
where
    S: Send + Sync,
{
    type Rejection = Infallible;

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        let properties = build_enrichment_properties(parts);
        Ok(Self { properties })
    }
}

trait ApplyApiKey {
    fn ensure_api_key(&mut self, api_key: &str);
}

fn apply_api_key<T: ApplyApiKey>(items: &mut [T], api_key: Option<&str>) {
    if let Some(api_key) = api_key {
        for item in items {
            item.ensure_api_key(api_key);
        }
    }
}

fn build_enrichment_properties(parts: &Parts) -> Map<String, Value> {
    let headers = &parts.headers;
    let mut props = Map::new();

    if let Some(ip) = header_value(headers, "cf-connecting-ip") {
        props.insert("$ip".to_string(), Value::String(ip));
    }

    if let Some(ray) = header_value(headers, "cf-ray") {
        props.insert("cf_ray".to_string(), Value::String(ray));
    }

    #[cfg(target_arch = "wasm32")]
    if let Some(cf) = parts.extensions.get::<Cf>() {
        insert_cf_properties(&mut props, cf);
    }

    props
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

#[cfg(target_arch = "wasm32")]
fn insert_cf_properties(props: &mut Map<String, Value>, cf: &Cf) {
    if let Some(country) = cf.country() {
        props.insert("$geoip_country_code".to_string(), Value::String(country));
    }

    if let Some(city) = cf.city() {
        props.insert("$geoip_city_name".to_string(), Value::String(city));
    }

    if let Some(continent) = cf.continent() {
        props.insert(
            "$geoip_continent_code".to_string(),
            Value::String(continent),
        );
    }

    if let Some((lat, lon)) = cf.coordinates() {
        if let Some(lat) = serde_json::Number::from_f64(lat as f64) {
            props.insert("$geoip_latitude".to_string(), Value::Number(lat));
        }
        if let Some(lon) = serde_json::Number::from_f64(lon as f64) {
            props.insert("$geoip_longitude".to_string(), Value::Number(lon));
        }
    }

    if let Some(postal_code) = cf.postal_code() {
        props.insert("$geoip_postal_code".to_string(), Value::String(postal_code));
    }

    if let Some(region_code) = cf.region_code() {
        props.insert(
            "$geoip_subdivision_1_code".to_string(),
            Value::String(region_code),
        );
    }

    if let Some(region) = cf.region() {
        props.insert(
            "$geoip_subdivision_1_name".to_string(),
            Value::String(region),
        );
    }

    let timezone = cf.timezone_name();
    if !timezone.is_empty() {
        props.insert("$geoip_time_zone".to_string(), Value::String(timezone));
    }

    if let Some(asn) = cf.asn() {
        props.insert(
            "cf_asn".to_string(),
            Value::Number(serde_json::Number::from(asn as u64)),
        );
    }

    if let Some(org) = cf.as_organization() {
        props.insert("cf_as_organization".to_string(), Value::String(org));
    }

    let colo = cf.colo();
    if !colo.is_empty() {
        props.insert("cf_colo".to_string(), Value::String(colo));
    }

    if let Some(metro_code) = cf.metro_code() {
        props.insert("cf_metro_code".to_string(), Value::String(metro_code));
    }
}

#[derive(Debug, Error)]
pub enum PayloadExtractorError {
    #[error("failed to read request body: {0}")]
    BodyRead(#[source] axum::Error),
    #[error("unsupported content encoding: {0}")]
    UnsupportedEncoding(String),
    #[error("failed to decode gzip payload: {0}")]
    Gzip(#[source] std::io::Error),
    #[error("failed to decode zlib payload: {0}")]
    Zlib(#[source] std::io::Error),
    #[error("failed to parse form payload: {0}")]
    Form(#[source] serde_urlencoded::de::Error),
    #[error("missing data field in PostHog payload")]
    MissingData,
    #[error("failed to parse JSON payload: {0}")]
    Json(#[source] serde_json::Error),
    #[error("expected JSON object or array in PostHog payload")]
    Structure(&'static str),
    #[error("unsupported compression algorithm: {0}")]
    UnsupportedCompression(String),
    #[error("missing signature header")]
    MissingSignature,
    #[error("signature verification failed")]
    InvalidSignature,
}

impl IntoResponse for PayloadExtractorError {
    fn into_response(self) -> axum::response::Response {
        warn!(error = %self, "failed to parse PostHog payload");
        match self {
            PayloadExtractorError::MissingSignature | PayloadExtractorError::InvalidSignature => {
                let body = Json(ErrorResponse {
                    status: 0,
                    error: self.to_string(),
                });
                (StatusCode::UNAUTHORIZED, body).into_response()
            }
            _ => {
                let body = Json(ErrorResponse {
                    status: 0,
                    error: format!("invalid payload: {self}"),
                });
                (StatusCode::BAD_REQUEST, body).into_response()
            }
        }
    }
}

#[async_trait]
impl<T> FromRequest<AppState, Body> for PostHogPayload<T>
where
    T: DeserializeOwned + ApplyApiKey,
{
    type Rejection = PayloadExtractorError;

    async fn from_request(
        request: Request<Body>,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let (parts, body) = request.into_parts();
        let headers = parts.headers;
        let bytes = to_bytes(body, usize::MAX)
            .await
            .map_err(PayloadExtractorError::BodyRead)?;

        verify_signature(&headers, &bytes, state.signing_secret.as_deref())?;
        let decoded = decode_content_encoding(&headers, &bytes)?;
        let mut payloads = parse_posthog_body::<T>(&headers, &decoded)?;
        let header_api_key = header_api_key(&headers);
        apply_api_key(&mut payloads, header_api_key.as_deref());
        let sent_at = header_sent_at(&headers);

        Ok(PostHogPayload {
            items: payloads,
            sent_at,
        })
    }
}

#[async_trait]
impl FromRequest<AppState, Body> for PostHogBatchPayload {
    type Rejection = PayloadExtractorError;

    async fn from_request(
        request: Request<Body>,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let (parts, body) = request.into_parts();
        let headers = parts.headers;
        let bytes = to_bytes(body, usize::MAX)
            .await
            .map_err(PayloadExtractorError::BodyRead)?;

        verify_signature(&headers, &bytes, state.signing_secret.as_deref())?;
        let decoded = decode_content_encoding(&headers, &bytes)?;
        let mut payload = parse_posthog_batch_body(&headers, &decoded)?;
        let header_api_key = header_api_key(&headers);
        if payload.api_key.is_none() {
            payload.api_key = header_api_key;
        }
        if payload.sent_at.is_none() {
            payload.sent_at = header_sent_at(&headers);
        }

        Ok(PostHogBatchPayload { batch: payload })
    }
}

fn decode_content_encoding(
    headers: &HeaderMap,
    body: &[u8],
) -> Result<Vec<u8>, PayloadExtractorError> {
    match headers
        .get(header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim().to_ascii_lowercase())
    {
        Some(ref encoding) if encoding == "gzip" => decompress_gzip(body),
        Some(ref encoding) if encoding == "deflate" || encoding == "zlib" => decompress_zlib(body),
        Some(ref encoding) if encoding.is_empty() => Ok(body.to_vec()),
        Some(encoding) => Err(PayloadExtractorError::UnsupportedEncoding(encoding)),
        None => Ok(body.to_vec()),
    }
}

pub(crate) fn header_api_key(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-posthog-api-key")
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_string())
}

pub(crate) fn header_sent_at(headers: &HeaderMap) -> Option<DateTime<Utc>> {
    headers
        .get("x-posthog-sent-at")
        .and_then(|value| value.to_str().ok())
        .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
        .map(|dt| dt.with_timezone(&Utc))
}

pub(crate) fn verify_signature(
    headers: &HeaderMap,
    body: &[u8],
    signing_secret: Option<&str>,
) -> Result<(), PayloadExtractorError> {
    let Some(secret) = signing_secret else {
        return Ok(());
    };

    let mut provided = Vec::new();

    if let Some(header) = headers.get("x-posthog-signature") {
        let raw = header
            .to_str()
            .map_err(|_| PayloadExtractorError::InvalidSignature)?
            .trim();
        let (algo, signature) = parse_signature(raw, "sha256");
        provided.push((algo.to_string(), signature.to_string()));
    }

    if let Some(header) = headers.get("x-hub-signature") {
        let raw = header
            .to_str()
            .map_err(|_| PayloadExtractorError::InvalidSignature)?
            .trim();
        let (algo, signature) = parse_signature(raw, "sha1");
        provided.push((algo.to_string(), signature.to_string()));
    }

    if provided.is_empty() {
        return Err(PayloadExtractorError::MissingSignature);
    }

    for (algo, signature) in provided {
        if verify_hmac(body, secret, &algo, &signature) {
            return Ok(());
        }
    }

    Err(PayloadExtractorError::InvalidSignature)
}

fn parse_signature<'a>(value: &'a str, default_algo: &'a str) -> (&'a str, &'a str) {
    if let Some((algo, signature)) = value.split_once('=') {
        (algo.trim(), signature.trim())
    } else {
        (default_algo, value.trim())
    }
}

fn verify_hmac(body: &[u8], secret: &str, algo: &str, provided: &str) -> bool {
    match algo {
        "sha256" => {
            let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(secret.as_bytes()) else {
                return false;
            };
            mac.update(body);
            let expected = mac.finalize().into_bytes();
            constant_time_eq_hex(&expected, provided)
        }
        "sha1" => {
            let Ok(mut mac) = Hmac::<Sha1>::new_from_slice(secret.as_bytes()) else {
                return false;
            };
            mac.update(body);
            let expected = mac.finalize().into_bytes();
            constant_time_eq_hex(&expected, provided)
        }
        _ => false,
    }
}

fn constant_time_eq_hex(expected: &[u8], provided: &str) -> bool {
    let expected_hex = hex_encode(expected);
    let cleaned = provided
        .trim()
        .trim_start_matches("sha256=")
        .trim_start_matches("sha1=");
    expected_hex.as_bytes().ct_eq(cleaned.as_bytes()).into()
}

fn parse_posthog_body<T>(headers: &HeaderMap, body: &[u8]) -> Result<Vec<T>, PayloadExtractorError>
where
    T: DeserializeOwned,
{
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(|value| {
            value
                .split(';')
                .next()
                .unwrap_or(value)
                .trim()
                .to_ascii_lowercase()
        });

    let is_form = matches!(
        content_type.as_deref(),
        Some("application/x-www-form-urlencoded")
    );

    if is_form || body.starts_with(b"data=") {
        parse_form_payload(body)
    } else {
        parse_json_payload(body)
    }
}

fn parse_posthog_batch_body(
    headers: &HeaderMap,
    body: &[u8],
) -> Result<BatchRequest, PayloadExtractorError> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(|value| {
            value
                .split(';')
                .next()
                .unwrap_or(value)
                .trim()
                .to_ascii_lowercase()
        });

    let is_form = matches!(
        content_type.as_deref(),
        Some("application/x-www-form-urlencoded")
    );

    if is_form || body.starts_with(b"data=") {
        parse_form_batch_payload(body)
    } else {
        parse_json_batch_payload(body)
    }
}

fn parse_form_payload<T>(body: &[u8]) -> Result<Vec<T>, PayloadExtractorError>
where
    T: DeserializeOwned,
{
    let form_pairs: Vec<(String, String)> =
        serde_urlencoded::from_bytes(body).map_err(PayloadExtractorError::Form)?;

    let mut shared = Map::new();
    let mut data_value: Option<Value> = None;
    let mut compression: Option<String> = None;

    for (key, value) in form_pairs {
        match key.as_str() {
            "data" => data_value = Some(Value::String(value)),
            "compression" | "compression_method" => compression = Some(value),
            other => {
                shared.insert(other.to_string(), Value::String(value));
            }
        }
    }

    let data = data_value.ok_or(PayloadExtractorError::MissingData)?;
    let payloads = decode_data_value(data, compression.as_deref())?;
    deserialize_events(payloads, shared)
}

fn parse_form_batch_payload(body: &[u8]) -> Result<BatchRequest, PayloadExtractorError> {
    let form_pairs: Vec<(String, String)> =
        serde_urlencoded::from_bytes(body).map_err(PayloadExtractorError::Form)?;

    let mut map = Map::new();
    let mut data_value: Option<Value> = None;
    let mut compression: Option<String> = None;

    for (key, value) in form_pairs {
        match key.as_str() {
            "data" => data_value = Some(Value::String(value)),
            "compression" | "compression_method" => compression = Some(value),
            other => {
                map.insert(other.to_string(), Value::String(value));
            }
        }
    }

    let data = data_value.ok_or(PayloadExtractorError::MissingData)?;
    let content = decode_data_content(data, compression.as_deref())?;
    apply_batch_data(content, &mut map)?;

    serde_json::from_value(Value::Object(map)).map_err(PayloadExtractorError::Json)
}

fn parse_json_payload<T>(body: &[u8]) -> Result<Vec<T>, PayloadExtractorError>
where
    T: DeserializeOwned,
{
    let value: Value = serde_json::from_slice(body).map_err(PayloadExtractorError::Json)?;
    match value {
        Value::Array(array) => array
            .into_iter()
            .map(|value| serde_json::from_value(value).map_err(PayloadExtractorError::Json))
            .collect(),
        Value::Object(mut map) => {
            let compression = map
                .remove("compression")
                .or_else(|| map.remove("compression_method"));
            if let Some(data) = map.remove("data") {
                let compression_str = compression
                    .as_ref()
                    .and_then(|value| value.as_str())
                    .map(|value| value.to_string());
                let payloads = decode_data_value(data, compression_str.as_deref())?;
                deserialize_events(payloads, map)
            } else {
                let event = Value::Object(map);
                Ok(vec![
                    serde_json::from_value(event).map_err(PayloadExtractorError::Json)?
                ])
            }
        }
        _ => Err(PayloadExtractorError::Structure(
            "expected object or array JSON payload",
        )),
    }
}

fn parse_json_batch_payload(body: &[u8]) -> Result<BatchRequest, PayloadExtractorError> {
    let value: Value = serde_json::from_slice(body).map_err(PayloadExtractorError::Json)?;
    match value {
        Value::Object(mut map) => {
            let compression = map
                .remove("compression")
                .or_else(|| map.remove("compression_method"));
            if let Some(data) = map.remove("data") {
                let compression_str = compression
                    .as_ref()
                    .and_then(|value| value.as_str())
                    .map(|value| value.to_string());
                let content = decode_data_content(data, compression_str.as_deref())?;
                apply_batch_data(content, &mut map)?;
            }

            serde_json::from_value(Value::Object(map)).map_err(PayloadExtractorError::Json)
        }
        _ => Err(PayloadExtractorError::Structure(
            "expected JSON object payload for batch endpoint",
        )),
    }
}

fn decode_data_value(
    data: Value,
    compression: Option<&str>,
) -> Result<Vec<Value>, PayloadExtractorError> {
    match decode_data_content(data, compression)? {
        Value::Array(array) => Ok(array),
        Value::Object(map) => Ok(vec![Value::Object(map)]),
        _ => Err(PayloadExtractorError::Structure(
            "expected JSON object or array inside data field",
        )),
    }
}

fn decode_data_content(
    data: Value,
    compression: Option<&str>,
) -> Result<Value, PayloadExtractorError> {
    match data {
        Value::Array(array) => Ok(Value::Array(array)),
        Value::Object(map) => Ok(Value::Object(map)),
        Value::String(string) => decode_data_string(&string, compression),
        _ => Err(PayloadExtractorError::Structure(
            "expected JSON object or array inside data field",
        )),
    }
}

fn decode_data_string(
    data: &str,
    compression: Option<&str>,
) -> Result<Value, PayloadExtractorError> {
    let raw = BASE64_STANDARD
        .decode(data.as_bytes())
        .unwrap_or_else(|_| data.as_bytes().to_vec());

    let decoded = match compression.map(|value| value.to_ascii_lowercase()) {
        Some(ref algo) if algo == "gzip" => decompress_gzip(&raw)?,
        Some(ref algo) if algo == "gzip-js" || algo == "zlib" || algo == "deflate" => {
            decompress_zlib(&raw)?
        }
        Some(algo) => return Err(PayloadExtractorError::UnsupportedCompression(algo)),
        None => raw.clone(),
    };

    match serde_json::from_slice::<Value>(&decoded) {
        Ok(value) => convert_embedded_value(value),
        Err(mut err) => {
            if compression.is_none() {
                if let Ok(zlib_decoded) = decompress_zlib(&raw) {
                    if let Ok(value) = serde_json::from_slice::<Value>(&zlib_decoded) {
                        return convert_embedded_value(value);
                    }
                }

                if let Ok(gzip_decoded) = decompress_gzip(&raw) {
                    if let Ok(value) = serde_json::from_slice::<Value>(&gzip_decoded) {
                        return convert_embedded_value(value);
                    }
                }
            }

            err = serde_json::from_slice::<Value>(&decoded).unwrap_err();
            Err(PayloadExtractorError::Json(err))
        }
    }
}

fn convert_embedded_value(value: Value) -> Result<Value, PayloadExtractorError> {
    match value {
        Value::Array(_) | Value::Object(_) => Ok(value),
        _ => Err(PayloadExtractorError::Structure(
            "expected JSON object or array inside data field",
        )),
    }
}

fn apply_batch_data(
    content: Value,
    target: &mut Map<String, Value>,
) -> Result<(), PayloadExtractorError> {
    match content {
        Value::Array(array) => {
            target.insert("batch".to_string(), Value::Array(array));
        }
        Value::Object(mut object) => {
            let batch_values = if let Some(batch_value) = object.remove("batch") {
                normalize_batch_array(batch_value)?
            } else {
                vec![Value::Object(object.clone())]
            };

            target.insert("batch".to_string(), Value::Array(batch_values));

            for (key, value) in object {
                target.entry(key).or_insert(value);
            }
        }
        _ => {
            return Err(PayloadExtractorError::Structure(
                "expected JSON object or array inside data field",
            ))
        }
    }

    Ok(())
}

fn normalize_batch_array(value: Value) -> Result<Vec<Value>, PayloadExtractorError> {
    match value {
        Value::Array(array) => Ok(array),
        Value::Object(map) => Ok(vec![Value::Object(map)]),
        _ => Err(PayloadExtractorError::Structure(
            "expected JSON array inside batch data",
        )),
    }
}

fn deserialize_events<T>(
    events: Vec<Value>,
    shared: Map<String, Value>,
) -> Result<Vec<T>, PayloadExtractorError>
where
    T: DeserializeOwned,
{
    events
        .into_iter()
        .map(|event| match event {
            Value::Object(mut object) => {
                for (key, value) in &shared {
                    object.entry(key.clone()).or_insert(value.clone());
                }
                serde_json::from_value(Value::Object(object)).map_err(PayloadExtractorError::Json)
            }
            _ => Err(PayloadExtractorError::Structure(
                "expected JSON object inside data field",
            )),
        })
        .collect()
}

fn decompress_gzip(data: &[u8]) -> Result<Vec<u8>, PayloadExtractorError> {
    let mut decoder = GzDecoder::new(data);
    let mut output = Vec::new();
    decoder
        .read_to_end(&mut output)
        .map_err(PayloadExtractorError::Gzip)?;
    Ok(output)
}

fn decompress_zlib(data: &[u8]) -> Result<Vec<u8>, PayloadExtractorError> {
    let mut decoder = ZlibDecoder::new(data);
    let mut output = Vec::new();
    decoder
        .read_to_end(&mut output)
        .map_err(PayloadExtractorError::Zlib)?;
    Ok(output)
}

impl ApplyApiKey for CaptureRequest {
    fn ensure_api_key(&mut self, api_key: &str) {
        if self.api_key.is_none() {
            self.api_key = Some(api_key.to_string());
        }
    }
}

impl ApplyApiKey for IdentifyRequest {
    fn ensure_api_key(&mut self, api_key: &str) {
        if self.api_key.is_none() {
            self.api_key = Some(api_key.to_string());
        }
    }
}

impl ApplyApiKey for GroupIdentifyRequest {
    fn ensure_api_key(&mut self, api_key: &str) {
        if self.api_key.is_none() {
            self.api_key = Some(api_key.to_string());
        }
    }
}

impl ApplyApiKey for AliasRequest {
    fn ensure_api_key(&mut self, api_key: &str) {
        if self.api_key.is_none() {
            self.api_key = Some(api_key.to_string());
        }
    }
}

impl ApplyApiKey for EngageRequest {
    fn ensure_api_key(&mut self, api_key: &str) {
        if self.api_key.is_none() {
            self.api_key = Some(api_key.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{header, Request};
    use chrono::TimeZone;
    use flate2::write::{GzEncoder, ZlibEncoder};
    use flate2::Compression;
    use serde_json::{json, Value};
    use std::{io::Write, sync::Arc, time::Duration};

    use crate::{
        models::CaptureRequest,
        pipeline::PipelineClient,
        persons::NoopPersonStore,
        AppState,
    };
    use reqwest::Url;

    fn test_state() -> AppState {
        let pipeline = PipelineClient::new(
            Url::parse("http://localhost:0").unwrap(),
            None,
            Duration::from_secs(1),
        )
        .unwrap();

        AppState {
            pipeline: Arc::new(pipeline),
            posthog_team_id: None,
            decide_api_token: None,
            session_recording_endpoint: None,
            signing_secret: None,
            person_store: Arc::new(NoopPersonStore),
            person_debug_token: None,
            group_store: Arc::new(crate::groups::NoopGroupStore),
            group_type_map: crate::groups::GroupTypeMap::default(),
            feature_flags: Arc::new(crate::feature_flags::FeatureFlagStore::empty()),
        }
    }

    #[tokio::test]
    async fn parses_json_payload() {
        let body = json!({
            "event": "test",
            "distinct_id": "abc",
            "api_key": "phc_123"
        })
        .to_string();

        let request = Request::builder()
            .uri("/capture")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .unwrap();

        let state = test_state();
        let payload: PostHogPayload<CaptureRequest> =
            PostHogPayload::from_request(request, &state).await.unwrap();

        assert_eq!(payload.items.len(), 1);
        assert_eq!(payload.items[0].event, "test");
        assert_eq!(payload.items[0].distinct_id, "abc");
        assert_eq!(payload.items[0].api_key.as_deref(), Some("phc_123"));
    }

    #[tokio::test]
    async fn parses_form_encoded_payload() {
        let event = json!({
            "event": "form-test",
            "distinct_id": "user",
        })
        .to_string();
        let encoded = BASE64_STANDARD.encode(event);
        let body = format!("data={}&api_key=phc_form", encoded);

        let request = Request::builder()
            .uri("/capture")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(body))
            .unwrap();

        let state = test_state();
        let payload: PostHogPayload<CaptureRequest> =
            PostHogPayload::from_request(request, &state).await.unwrap();

        assert_eq!(payload.items.len(), 1);
        assert_eq!(payload.items[0].event, "form-test");
        assert_eq!(payload.items[0].api_key.as_deref(), Some("phc_form"));
    }

    #[tokio::test]
    async fn parses_gzipped_payload() {
        let body = json!({
            "event": "gzip-test",
            "distinct_id": "123",
        })
        .to_string();
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(body.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();

        let request = Request::builder()
            .uri("/capture")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CONTENT_ENCODING, "gzip")
            .body(Body::from(compressed))
            .unwrap();

        let state = test_state();
        let payload: PostHogPayload<CaptureRequest> =
            PostHogPayload::from_request(request, &state).await.unwrap();

        assert_eq!(payload.items.len(), 1);
        assert_eq!(payload.items[0].event, "gzip-test");
        assert_eq!(payload.items[0].distinct_id, "123");
    }

    #[tokio::test]
    async fn parses_json_data_envelope() {
        let body = json!({
            "data": {
                "event": "wrapped",
                "distinct_id": "abc",
            },
            "api_key": "phc_wrapped"
        })
        .to_string();

        let request = Request::builder()
            .uri("/capture")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .unwrap();

        let state = test_state();
        let payload: PostHogPayload<CaptureRequest> =
            PostHogPayload::from_request(request, &state).await.unwrap();

        assert_eq!(payload.items.len(), 1);
        assert_eq!(payload.items[0].event, "wrapped");
        assert_eq!(payload.items[0].api_key.as_deref(), Some("phc_wrapped"));
    }

    #[tokio::test]
    async fn parses_form_payload_with_compression() {
        let event = json!({
            "event": "compressed-form",
            "distinct_id": "form-user",
        })
        .to_string();

        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(event.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();
        let encoded = BASE64_STANDARD.encode(compressed);
        let body = format!(
            "data={}&compression=gzip-js&api_key=phc_compressed",
            encoded
        );

        let request = Request::builder()
            .uri("/capture")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(body))
            .unwrap();

        let state = test_state();
        let payload: PostHogPayload<CaptureRequest> =
            PostHogPayload::from_request(request, &state).await.unwrap();

        assert_eq!(payload.items.len(), 1);
        assert_eq!(payload.items[0].event, "compressed-form");
        assert_eq!(payload.items[0].distinct_id, "form-user");
        assert_eq!(payload.items[0].api_key.as_deref(), Some("phc_compressed"));
    }

    #[tokio::test]
    async fn parses_json_payload_with_implicit_compression() {
        let event = json!({
            "event": "implicit-compression",
            "distinct_id": "json-user",
        })
        .to_string();

        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(event.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();
        let encoded = BASE64_STANDARD.encode(compressed);

        let body = json!({
            "data": encoded,
            "api_key": "phc_json_compressed"
        })
        .to_string();

        let request = Request::builder()
            .uri("/capture")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .unwrap();

        let state = test_state();
        let payload: PostHogPayload<CaptureRequest> =
            PostHogPayload::from_request(request, &state).await.unwrap();

        assert_eq!(payload.items.len(), 1);
        assert_eq!(payload.items[0].event, "implicit-compression");
        assert_eq!(payload.items[0].distinct_id, "json-user");
        assert_eq!(
            payload.items[0].api_key.as_deref(),
            Some("phc_json_compressed")
        );
    }

    #[tokio::test]
    async fn parses_json_batch_payload() {
        let body = json!({
            "api_key": "phc_batch",
            "batch": [
                {
                    "event": "batched",
                    "distinct_id": "batched-user"
                }
            ],
            "sent_at": "2025-01-01T00:00:00Z"
        })
        .to_string();

        let request = Request::builder()
            .uri("/batch")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .unwrap();

        let state = test_state();
        let payload: PostHogBatchPayload = PostHogBatchPayload::from_request(request, &state)
            .await
            .unwrap();

        assert_eq!(payload.batch.api_key.as_deref(), Some("phc_batch"));
        assert_eq!(payload.batch.batch.len(), 1);
        assert_eq!(payload.batch.batch[0]["event"], "batched");
        assert_eq!(payload.batch.batch[0]["distinct_id"], "batched-user");
    }

    #[tokio::test]
    async fn parses_compressed_batch_payload() {
        let data_payload = json!({
            "batch": [
                {
                    "event": "wrapped-batch",
                    "distinct_id": "wrapped-user"
                }
            ],
            "sent_at": "2025-02-02T00:00:00Z"
        })
        .to_string();

        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(data_payload.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();
        let encoded = BASE64_STANDARD.encode(compressed);

        let body = json!({
            "data": encoded,
            "compression": "gzip-js",
            "api_key": "phc_wrapped_batch"
        })
        .to_string();

        let request = Request::builder()
            .uri("/batch")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .unwrap();

        let state = test_state();
        let payload: PostHogBatchPayload = PostHogBatchPayload::from_request(request, &state)
            .await
            .unwrap();

        assert_eq!(payload.batch.api_key.as_deref(), Some("phc_wrapped_batch"));
        assert_eq!(payload.batch.batch.len(), 1);
        assert_eq!(payload.batch.batch[0]["event"], "wrapped-batch");
        assert_eq!(payload.batch.batch[0]["distinct_id"], "wrapped-user");
        let expected = chrono::Utc.with_ymd_and_hms(2025, 2, 2, 0, 0, 0).unwrap();
        assert_eq!(payload.batch.sent_at, Some(expected));
    }

    #[test]
    fn enrichment_reads_cloudflare_headers() {
        let request = Request::builder()
            .uri("/capture")
            .header("cf-connecting-ip", "203.0.113.10")
            .header("cf-ray", "ray-123")
            .body(Body::empty())
            .unwrap();
        let (parts, _) = request.into_parts();

        let props = build_enrichment_properties(&parts);

        assert_eq!(
            props.get("$ip"),
            Some(&Value::String("203.0.113.10".to_string()))
        );
        assert_eq!(
            props.get("cf_ray"),
            Some(&Value::String("ray-123".to_string()))
        );
    }

    #[test]
    fn enrichment_ignores_empty_header_values() {
        let request = Request::builder()
            .uri("/capture")
            .header("cf-connecting-ip", "   ")
            .header("cf-ray", "")
            .body(Body::empty())
            .unwrap();
        let (parts, _) = request.into_parts();

        let props = build_enrichment_properties(&parts);

        assert!(props.get("$ip").is_none());
        assert!(props.get("cf_ray").is_none());
    }
}
