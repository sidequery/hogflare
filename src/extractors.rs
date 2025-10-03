use std::io::Read;

use axum::async_trait;
use axum::body::{to_bytes, Body};
use axum::extract::FromRequest;
use axum::http::{header, HeaderMap, Request, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use flate2::read::{GzDecoder, ZlibDecoder};
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};
use serde_urlencoded;
use thiserror::Error;
use tracing::warn;

use crate::models::ErrorResponse;

pub struct PostHogPayload<T>(pub Vec<T>);

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
}

impl IntoResponse for PayloadExtractorError {
    fn into_response(self) -> axum::response::Response {
        warn!(error = %self, "failed to parse PostHog payload");
        let body = Json(ErrorResponse {
            status: 0,
            error: format!("invalid payload: {self}"),
        });
        (StatusCode::BAD_REQUEST, body).into_response()
    }
}

#[async_trait]
impl<S, T> FromRequest<S, Body> for PostHogPayload<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = PayloadExtractorError;

    async fn from_request(request: Request<Body>, _state: &S) -> Result<Self, Self::Rejection> {
        let (parts, body) = request.into_parts();
        let headers = parts.headers;
        let bytes = to_bytes(body, usize::MAX)
            .await
            .map_err(PayloadExtractorError::BodyRead)?;

        let decoded = decode_content_encoding(&headers, &bytes)?;
        let payloads = parse_posthog_body::<T>(&headers, &decoded)?;

        Ok(PostHogPayload(payloads))
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

fn decode_data_value(
    data: Value,
    compression: Option<&str>,
) -> Result<Vec<Value>, PayloadExtractorError> {
    match data {
        Value::Array(array) => Ok(array),
        Value::Object(map) => Ok(vec![Value::Object(map)]),
        Value::String(string) => decode_data_string(&string, compression),
        _ => Err(PayloadExtractorError::Structure(
            "expected JSON object or array inside data field",
        )),
    }
}

fn decode_data_string(
    data: &str,
    compression: Option<&str>,
) -> Result<Vec<Value>, PayloadExtractorError> {
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

fn convert_embedded_value(value: Value) -> Result<Vec<Value>, PayloadExtractorError> {
    match value {
        Value::Array(array) => Ok(array),
        Value::Object(map) => Ok(vec![Value::Object(map)]),
        _ => Err(PayloadExtractorError::Structure(
            "expected JSON object or array inside data field",
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{header, Request};
    use flate2::write::{GzEncoder, ZlibEncoder};
    use flate2::Compression;
    use serde_json::json;
    use std::io::Write;

    use crate::models::CaptureRequest;

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

        let PostHogPayload(payloads): PostHogPayload<CaptureRequest> =
            PostHogPayload::from_request(request, &()).await.unwrap();

        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0].event, "test");
        assert_eq!(payloads[0].distinct_id, "abc");
        assert_eq!(payloads[0].api_key.as_deref(), Some("phc_123"));
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

        let PostHogPayload(payloads): PostHogPayload<CaptureRequest> =
            PostHogPayload::from_request(request, &()).await.unwrap();

        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0].event, "form-test");
        assert_eq!(payloads[0].api_key.as_deref(), Some("phc_form"));
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

        let PostHogPayload(payloads): PostHogPayload<CaptureRequest> =
            PostHogPayload::from_request(request, &()).await.unwrap();

        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0].event, "gzip-test");
        assert_eq!(payloads[0].distinct_id, "123");
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

        let PostHogPayload(payloads): PostHogPayload<CaptureRequest> =
            PostHogPayload::from_request(request, &()).await.unwrap();

        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0].event, "wrapped");
        assert_eq!(payloads[0].api_key.as_deref(), Some("phc_wrapped"));
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

        let PostHogPayload(payloads): PostHogPayload<CaptureRequest> =
            PostHogPayload::from_request(request, &()).await.unwrap();

        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0].event, "compressed-form");
        assert_eq!(payloads[0].distinct_id, "form-user");
        assert_eq!(payloads[0].api_key.as_deref(), Some("phc_compressed"));
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

        let PostHogPayload(payloads): PostHogPayload<CaptureRequest> =
            PostHogPayload::from_request(request, &()).await.unwrap();

        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0].event, "implicit-compression");
        assert_eq!(payloads[0].distinct_id, "json-user");
        assert_eq!(payloads[0].api_key.as_deref(), Some("phc_json_compressed"));
    }
}
