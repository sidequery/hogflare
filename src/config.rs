use std::{net::SocketAddr, time::Duration};

#[cfg(not(target_arch = "wasm32"))]
use std::env::{self, VarError};

use url::Url;
use thiserror::Error;

use crate::feature_flags::FeatureFlagStore;

#[derive(Debug, Clone)]
pub struct Config {
    pub address: SocketAddr,
    pub pipeline_endpoint: Url,
    pub pipeline_auth_token: Option<String>,
    pub pipeline_timeout: Duration,
    pub posthog_team_id: Option<i64>,
    pub posthog_group_types: [Option<String>; 5],
    pub posthog_project_api_key: Option<String>,
    pub session_recording_endpoint: Option<String>,
    pub posthog_signing_secret: Option<String>,
    pub person_debug_token: Option<String>,
    pub feature_flags: FeatureFlagStore,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("environment variable {0} is required")]
    MissingVar(&'static str),
    #[error("failed to parse socket address `{value}`: {message}")]
    InvalidSocketAddress { value: String, message: String },
    #[error("invalid pipeline endpoint `{value}`: {message}")]
    InvalidEndpoint { value: String, message: String },
    #[error("failed to parse pipeline timeout `{value}`: {message}")]
    InvalidTimeout { value: String, message: String },
    #[error("failed to parse POSTHOG_TEAM_ID `{value}`: {message}")]
    InvalidTeamId { value: String, message: String },
    #[error("failed to parse HOGFLARE_FEATURE_FLAGS: {0}")]
    InvalidFeatureFlags(String),
}

impl Config {
    #[cfg(target_arch = "wasm32")]
    pub fn from_worker_env(env: &worker::Env) -> Result<Self, ConfigError> {
        let address: SocketAddr = "0.0.0.0:8080"
            .parse::<SocketAddr>()
            .map_err(|err| {
            ConfigError::InvalidSocketAddress {
                value: "0.0.0.0:8080".to_string(),
                message: err.to_string(),
            }
        })?;

        let raw_endpoint = env
            .var("CLOUDFLARE_PIPELINE_ENDPOINT")
            .map_err(|_| ConfigError::MissingVar("CLOUDFLARE_PIPELINE_ENDPOINT"))?
            .to_string();

        let pipeline_endpoint =
            Url::parse(&raw_endpoint).map_err(|err| ConfigError::InvalidEndpoint {
                value: raw_endpoint.clone(),
                message: err.to_string(),
            })?;

        let pipeline_auth_token = env
            .secret("CLOUDFLARE_PIPELINE_AUTH_TOKEN")
            .ok()
            .map(|secret| secret.to_string())
            .or_else(|| {
                env.var("CLOUDFLARE_PIPELINE_AUTH_TOKEN")
                    .ok()
                    .map(|var| var.to_string())
            });

        let pipeline_timeout = match env.var("CLOUDFLARE_PIPELINE_TIMEOUT_SECS") {
            Ok(value) => value
                .to_string()
                .parse::<u64>()
                .map(Duration::from_secs)
                .map_err(|err| ConfigError::InvalidTimeout {
                    value: value.to_string(),
                    message: err.to_string(),
                })?,
            Err(_) => Duration::from_secs(10),
        };

        let posthog_group_types = [
            env.var("POSTHOG_GROUP_TYPE_0").ok().map(|v| v.to_string()),
            env.var("POSTHOG_GROUP_TYPE_1").ok().map(|v| v.to_string()),
            env.var("POSTHOG_GROUP_TYPE_2").ok().map(|v| v.to_string()),
            env.var("POSTHOG_GROUP_TYPE_3").ok().map(|v| v.to_string()),
            env.var("POSTHOG_GROUP_TYPE_4").ok().map(|v| v.to_string()),
        ];

        let posthog_team_id = match env.var("POSTHOG_TEAM_ID") {
            Ok(value) => Some(
                value
                    .to_string()
                    .parse::<i64>()
                    .map_err(|err| ConfigError::InvalidTeamId {
                        value: value.to_string(),
                        message: err.to_string(),
                    })?,
            ),
            Err(_) => None,
        };

        let posthog_project_api_key = env.var("POSTHOG_API_KEY").ok().map(|v| v.to_string());
        let session_recording_endpoint = env
            .var("POSTHOG_SESSION_RECORDING_ENDPOINT")
            .ok()
            .map(|v| v.to_string());
        let posthog_signing_secret = env
            .secret("POSTHOG_SIGNING_SECRET")
            .ok()
            .map(|secret| secret.to_string())
            .or_else(|| env.var("POSTHOG_SIGNING_SECRET").ok().map(|v| v.to_string()));
        let person_debug_token = env.var("PERSON_DEBUG_TOKEN").ok().map(|v| v.to_string());
        let feature_flags_raw = env
            .secret("HOGFLARE_FEATURE_FLAGS")
            .ok()
            .map(|secret| secret.to_string())
            .or_else(|| {
                env.var("HOGFLARE_FEATURE_FLAGS")
                    .ok()
                    .map(|var| var.to_string())
            });

        let feature_flags = match feature_flags_raw {
            Some(value) => FeatureFlagStore::from_json(&value)
                .map_err(|err| ConfigError::InvalidFeatureFlags(err.to_string()))?,
            None => FeatureFlagStore::empty(),
        };

        Ok(Self {
            address,
            pipeline_endpoint,
            pipeline_auth_token,
            pipeline_timeout,
            posthog_team_id,
            posthog_group_types,
            posthog_project_api_key,
            session_recording_endpoint,
            posthog_signing_secret,
            person_debug_token,
            feature_flags,
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn from_env() -> Result<Self, ConfigError> {
        let address_raw = env::var("APP_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
        let address: SocketAddr =
            address_raw
                .parse()
                .map_err(
                    |err: std::net::AddrParseError| ConfigError::InvalidSocketAddress {
                        value: address_raw.clone(),
                        message: err.to_string(),
                    },
                )?;

        let raw_endpoint = env::var("CLOUDFLARE_PIPELINE_ENDPOINT")
            .map_err(|_| ConfigError::MissingVar("CLOUDFLARE_PIPELINE_ENDPOINT"))?;

        let pipeline_endpoint =
            Url::parse(&raw_endpoint).map_err(|err| ConfigError::InvalidEndpoint {
                value: raw_endpoint.clone(),
                message: err.to_string(),
            })?;

        let pipeline_auth_token = env::var("CLOUDFLARE_PIPELINE_AUTH_TOKEN").ok();

        let pipeline_timeout = match env::var("CLOUDFLARE_PIPELINE_TIMEOUT_SECS") {
            Ok(value) => value
                .parse::<u64>()
                .map(Duration::from_secs)
                .map_err(|err| ConfigError::InvalidTimeout {
                    value,
                    message: err.to_string(),
                })?,
            Err(VarError::NotPresent) => Duration::from_secs(10),
            Err(VarError::NotUnicode(_)) => {
                return Err(ConfigError::InvalidTimeout {
                    value: "<invalid-unicode>".to_string(),
                    message: "value contains invalid unicode".to_string(),
                })
            }
        };

        let posthog_group_types = [
            env::var("POSTHOG_GROUP_TYPE_0").ok(),
            env::var("POSTHOG_GROUP_TYPE_1").ok(),
            env::var("POSTHOG_GROUP_TYPE_2").ok(),
            env::var("POSTHOG_GROUP_TYPE_3").ok(),
            env::var("POSTHOG_GROUP_TYPE_4").ok(),
        ];

        let posthog_team_id = match env::var("POSTHOG_TEAM_ID") {
            Ok(value) => Some(
                value
                    .parse::<i64>()
                    .map_err(|err| ConfigError::InvalidTeamId {
                        value,
                        message: err.to_string(),
                    })?,
            ),
            Err(VarError::NotPresent) => None,
            Err(VarError::NotUnicode(_)) => {
                return Err(ConfigError::InvalidTeamId {
                    value: "<invalid-unicode>".to_string(),
                    message: "value contains invalid unicode".to_string(),
                })
            }
        };

        let posthog_project_api_key = env::var("POSTHOG_API_KEY").ok();
        let session_recording_endpoint = env::var("POSTHOG_SESSION_RECORDING_ENDPOINT").ok();
        let posthog_signing_secret = env::var("POSTHOG_SIGNING_SECRET").ok();
        let person_debug_token = env::var("PERSON_DEBUG_TOKEN").ok();
        let feature_flags = match env::var("HOGFLARE_FEATURE_FLAGS") {
            Ok(value) => FeatureFlagStore::from_json(&value)
                .map_err(|err| ConfigError::InvalidFeatureFlags(err.to_string()))?,
            Err(VarError::NotPresent) => FeatureFlagStore::empty(),
            Err(VarError::NotUnicode(_)) => {
                return Err(ConfigError::InvalidFeatureFlags(
                    "value contains invalid unicode".to_string(),
                ))
            }
        };

        Ok(Self {
            address,
            pipeline_endpoint,
            pipeline_auth_token,
            pipeline_timeout,
            posthog_team_id,
            posthog_group_types,
            posthog_project_api_key,
            session_recording_endpoint,
            posthog_signing_secret,
            person_debug_token,
            feature_flags,
        })
    }
}
