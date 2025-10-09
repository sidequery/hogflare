use std::{
    env::{self, VarError},
    net::SocketAddr,
    time::Duration,
};

use reqwest::Url;
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct Config {
    pub address: SocketAddr,
    pub pipeline_endpoint: Url,
    pub pipeline_auth_token: Option<String>,
    pub pipeline_timeout: Duration,
    pub posthog_project_api_key: Option<String>,
    pub session_recording_endpoint: Option<String>,
    pub posthog_signing_secret: Option<String>,
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
}

impl Config {
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

        let posthog_project_api_key = env::var("POSTHOG_API_KEY").ok();
        let session_recording_endpoint = env::var("POSTHOG_SESSION_RECORDING_ENDPOINT").ok();
        let posthog_signing_secret = env::var("POSTHOG_SIGNING_SECRET").ok();

        Ok(Self {
            address,
            pipeline_endpoint,
            pipeline_auth_token,
            pipeline_timeout,
            posthog_project_api_key,
            session_recording_endpoint,
            posthog_signing_secret,
        })
    }
}
