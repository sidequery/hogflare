use std::collections::{HashMap, HashSet};
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::time::{Duration, Instant};

use chrono::{DateTime, Duration as ChronoDuration, NaiveDate, Utc};
use http::StatusCode;
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use thiserror::Error;
use uuid::Uuid;

use crate::groups::GroupTypeMap;
use crate::pipeline::{PersonPipelineRecord, PipelineClient, PipelineError, PipelineEvent};

const DEFAULT_POSTHOG_HOST: &str = "https://us.posthog.com";
const DEFAULT_BATCH_SIZE: usize = 500;
const DEFAULT_TIMEOUT_SECS: u64 = 30;
const DEFAULT_IMPORT_STATE_FILE: &str = ".hogflare-import-state.jsonl";
const DEFAULT_TARGET_TABLE: &str = "default.hogflare_events_v3";
const DEFAULT_PERSONS_TARGET_TABLE: &str = "default.hogflare_persons_v2";
const DEFAULT_PIPELINE_FLUSH_SECS: u64 = 300;
const MIN_TARGET_WAIT_SECS: u64 = 60;
const TARGET_WAIT_GRACE_SECS: u64 = 30;
const PROGRESS_INTERVAL: usize = 10_000;
const POSTHOG_MAX_RETRIES: usize = 3;

#[derive(Debug, Clone)]
pub struct ImportConfig {
    pub posthog_host: Url,
    pub posthog_project_id: String,
    pub posthog_environment_id: Option<String>,
    pub posthog_personal_api_key: String,
    pub pipeline_endpoint: Url,
    pub pipeline_auth_token: Option<String>,
    pub persons_pipeline_endpoint: Option<Url>,
    pub persons_pipeline_auth_token: Option<String>,
    pub pipeline_timeout: Duration,
    pub hogflare_api_key: Option<String>,
    pub posthog_team_id: Option<i64>,
    pub posthog_group_types: [Option<String>; 5],
    pub batch_size: usize,
    pub persons_offset: usize,
    pub events_offset: usize,
    pub max_persons: Option<usize>,
    pub max_groups: Option<usize>,
    pub max_events: Option<usize>,
    pub events_after_timestamp: Option<DateTime<Utc>>,
    pub events_after_uuid: Option<String>,
    pub event_uuids_file: Option<String>,
    pub event_window_days: Option<i64>,
    pub event_window_hours: Option<i64>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    pub import_persons: bool,
    pub import_groups: bool,
    pub import_events: bool,
    pub emit_persons: bool,
    pub dry_run: bool,
    pub import_state_file: Option<String>,
    pub target_account_id: Option<String>,
    pub target_bucket: Option<String>,
    pub target_table: String,
    pub persons_target_table: String,
    pub target_auth_token: Option<String>,
    pub target_wait: Duration,
    pub target_poll: Duration,
    pub target_checks_enabled: bool,
    pub require_target_check: bool,
    pub target_wait_explicit: bool,
    pub pipeline_flush: Option<Duration>,
    pub cloudflare_api_token: Option<String>,
}

impl ImportConfig {
    pub fn from_env_and_args<I, S>(args: I) -> Result<Self, ImportError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let args = ImportArgs::parse(args)?;

        if args.help {
            return Err(ImportError::Usage(usage()));
        }

        let posthog_host = parse_url(
            "POSTHOG_HOST",
            args.posthog_host
                .or_else(|| env_var("POSTHOG_HOST"))
                .unwrap_or_else(|| DEFAULT_POSTHOG_HOST.to_string()),
        )?;
        let posthog_project_id = required(
            "POSTHOG_PROJECT_ID",
            args.posthog_project_id
                .or_else(|| env_var("POSTHOG_PROJECT_ID")),
        )?;
        let posthog_environment_id = args
            .posthog_environment_id
            .or_else(|| env_var("POSTHOG_ENVIRONMENT_ID"));
        let posthog_personal_api_key = required(
            "POSTHOG_PERSONAL_API_KEY",
            args.posthog_personal_api_key
                .or_else(|| env_var("POSTHOG_PERSONAL_API_KEY")),
        )?;
        let pipeline_endpoint = parse_url(
            "CLOUDFLARE_PIPELINE_ENDPOINT",
            required(
                "CLOUDFLARE_PIPELINE_ENDPOINT",
                args.pipeline_endpoint
                    .or_else(|| env_var("CLOUDFLARE_PIPELINE_ENDPOINT")),
            )?,
        )?;
        let pipeline_auth_token = args
            .pipeline_auth_token
            .or_else(|| env_var("CLOUDFLARE_PIPELINE_AUTH_TOKEN"));
        let persons_pipeline_endpoint = args
            .persons_pipeline_endpoint
            .or_else(|| env_var("IMPORT_PERSONS_PIPELINE_ENDPOINT"))
            .or_else(|| env_var("CLOUDFLARE_PERSONS_PIPELINE_ENDPOINT"))
            .map(|value| parse_url("CLOUDFLARE_PERSONS_PIPELINE_ENDPOINT", value))
            .transpose()?;
        let persons_pipeline_auth_token = args
            .persons_pipeline_auth_token
            .or_else(|| env_var("IMPORT_PERSONS_PIPELINE_AUTH_TOKEN"))
            .or_else(|| env_var("CLOUDFLARE_PERSONS_PIPELINE_AUTH_TOKEN"))
            .or_else(|| pipeline_auth_token.clone());
        let pipeline_timeout = parse_duration_secs(
            "CLOUDFLARE_PIPELINE_TIMEOUT_SECS",
            args.pipeline_timeout_secs
                .or_else(|| env_var("CLOUDFLARE_PIPELINE_TIMEOUT_SECS")),
            DEFAULT_TIMEOUT_SECS,
        )?;
        let hogflare_api_key = args
            .hogflare_api_key
            .or_else(|| env_var("HOGFLARE_API_KEY"))
            .or_else(|| env_var("POSTHOG_API_KEY"));
        let posthog_team_id = parse_optional_i64(
            "POSTHOG_TEAM_ID",
            args.team_id.or_else(|| env_var("POSTHOG_TEAM_ID")),
        )?;
        let posthog_group_types = [
            args.group_types[0]
                .clone()
                .or_else(|| env_var("POSTHOG_GROUP_TYPE_0")),
            args.group_types[1]
                .clone()
                .or_else(|| env_var("POSTHOG_GROUP_TYPE_1")),
            args.group_types[2]
                .clone()
                .or_else(|| env_var("POSTHOG_GROUP_TYPE_2")),
            args.group_types[3]
                .clone()
                .or_else(|| env_var("POSTHOG_GROUP_TYPE_3")),
            args.group_types[4]
                .clone()
                .or_else(|| env_var("POSTHOG_GROUP_TYPE_4")),
        ];

        let batch_size = parse_usize(
            "IMPORT_BATCH_SIZE",
            args.batch_size.or_else(|| env_var("IMPORT_BATCH_SIZE")),
            DEFAULT_BATCH_SIZE,
        )?;
        if batch_size == 0 {
            return Err(ImportError::InvalidConfig(
                "IMPORT_BATCH_SIZE must be greater than 0".to_string(),
            ));
        }
        let persons_offset = parse_usize(
            "IMPORT_PERSONS_OFFSET",
            args.persons_offset
                .or_else(|| env_var("IMPORT_PERSONS_OFFSET")),
            0,
        )?;
        let events_offset = parse_usize(
            "IMPORT_EVENTS_OFFSET",
            args.events_offset
                .or_else(|| env_var("IMPORT_EVENTS_OFFSET")),
            0,
        )?;
        let max_persons = parse_optional_usize(
            "IMPORT_MAX_PERSONS",
            args.max_persons.or_else(|| env_var("IMPORT_MAX_PERSONS")),
        )?;
        if matches!(max_persons, Some(0)) {
            return Err(ImportError::InvalidConfig(
                "IMPORT_MAX_PERSONS must be greater than 0".to_string(),
            ));
        }
        let max_groups = parse_optional_usize(
            "IMPORT_MAX_GROUPS",
            args.max_groups.or_else(|| env_var("IMPORT_MAX_GROUPS")),
        )?;
        if matches!(max_groups, Some(0)) {
            return Err(ImportError::InvalidConfig(
                "IMPORT_MAX_GROUPS must be greater than 0".to_string(),
            ));
        }
        let max_events = parse_optional_usize(
            "IMPORT_MAX_EVENTS",
            args.max_events.or_else(|| env_var("IMPORT_MAX_EVENTS")),
        )?;
        if matches!(max_events, Some(0)) {
            return Err(ImportError::InvalidConfig(
                "IMPORT_MAX_EVENTS must be greater than 0".to_string(),
            ));
        }
        let events_after_timestamp = parse_optional_datetime(
            "IMPORT_EVENTS_AFTER_TIMESTAMP",
            args.events_after_timestamp
                .or_else(|| env_var("IMPORT_EVENTS_AFTER_TIMESTAMP")),
        )?;
        let events_after_uuid = args
            .events_after_uuid
            .or_else(|| env_var("IMPORT_EVENTS_AFTER_UUID"));
        if events_after_uuid.is_some() && events_after_timestamp.is_none() {
            return Err(ImportError::InvalidConfig(
                "IMPORT_EVENTS_AFTER_UUID requires IMPORT_EVENTS_AFTER_TIMESTAMP".to_string(),
            ));
        }
        let event_uuids_file = args
            .event_uuids_file
            .or_else(|| env_var("IMPORT_EVENT_UUIDS_FILE"));
        let event_window_days = parse_optional_i64(
            "IMPORT_EVENT_WINDOW_DAYS",
            args.event_window_days
                .or_else(|| env_var("IMPORT_EVENT_WINDOW_DAYS")),
        )?;
        if matches!(event_window_days, Some(days) if days <= 0) {
            return Err(ImportError::InvalidConfig(
                "IMPORT_EVENT_WINDOW_DAYS must be greater than 0".to_string(),
            ));
        }
        let event_window_hours = parse_optional_i64(
            "IMPORT_EVENT_WINDOW_HOURS",
            args.event_window_hours
                .or_else(|| env_var("IMPORT_EVENT_WINDOW_HOURS")),
        )?;
        if matches!(event_window_hours, Some(hours) if hours <= 0) {
            return Err(ImportError::InvalidConfig(
                "IMPORT_EVENT_WINDOW_HOURS must be greater than 0".to_string(),
            ));
        }
        if event_window_days.is_some() && event_window_hours.is_some() {
            return Err(ImportError::InvalidConfig(
                "set only one of IMPORT_EVENT_WINDOW_DAYS or IMPORT_EVENT_WINDOW_HOURS".to_string(),
            ));
        }

        let from =
            parse_optional_datetime("IMPORT_FROM", args.from.or_else(|| env_var("IMPORT_FROM")))?;
        let to = parse_optional_datetime("IMPORT_TO", args.to.or_else(|| env_var("IMPORT_TO")))?;
        if let (Some(from), Some(to)) = (from, to) {
            if from >= to {
                return Err(ImportError::InvalidConfig(
                    "IMPORT_FROM must be before IMPORT_TO".to_string(),
                ));
            }
        }
        let import_state_file = if args.no_import_state {
            None
        } else {
            args.import_state_file
                .or_else(|| env_var("IMPORT_STATE_FILE"))
                .or_else(|| Some(DEFAULT_IMPORT_STATE_FILE.to_string()))
        };
        let target_account_id = args
            .target_account_id
            .or_else(|| env_var("IMPORT_TARGET_ACCOUNT_ID"))
            .or_else(|| env_var("CLOUDFLARE_ACCOUNT_ID"));
        let target_bucket = args
            .target_bucket
            .or_else(|| env_var("IMPORT_TARGET_BUCKET"))
            .or_else(|| env_var("R2_SQL_BUCKET"))
            .or_else(|| env_var("CLOUDFLARE_R2_BUCKET"));
        let target_auth_token = args
            .target_auth_token
            .or_else(|| env_var("IMPORT_TARGET_AUTH_TOKEN"))
            .or_else(|| env_var("WRANGLER_R2_SQL_AUTH_TOKEN"))
            .or_else(|| env_var("R2_SQL_AUTH_TOKEN"))
            .or_else(|| env_var("R2_DATA_CATALOG_TOKEN"));
        let target_table = parse_target_table(
            "IMPORT_TARGET_TABLE",
            args.target_table
                .or_else(|| env_var("IMPORT_TARGET_TABLE"))
                .or_else(|| env_var("R2_SQL_TABLE"))
                .unwrap_or_else(|| DEFAULT_TARGET_TABLE.to_string()),
        )?;
        let persons_target_table = parse_target_table(
            "IMPORT_PERSONS_TARGET_TABLE",
            args.persons_target_table
                .or_else(|| env_var("IMPORT_PERSONS_TARGET_TABLE"))
                .or_else(|| env_var("CLOUDFLARE_PERSONS_TARGET_TABLE"))
                .unwrap_or_else(|| DEFAULT_PERSONS_TARGET_TABLE.to_string()),
        )?;
        let pipeline_flush = parse_optional_duration_secs(
            "IMPORT_PIPELINE_FLUSH_SECS",
            args.pipeline_flush_secs
                .or_else(|| env_var("IMPORT_PIPELINE_FLUSH_SECS")),
        )?;
        let target_wait_value = args
            .target_wait_secs
            .or_else(|| env_var("IMPORT_TARGET_WAIT_SECS"));
        let target_wait_explicit = target_wait_value.is_some();
        let target_wait = match target_wait_value {
            Some(value) => parse_duration_secs("IMPORT_TARGET_WAIT_SECS", Some(value), 0)?,
            None => target_wait_for_flush(
                pipeline_flush
                    .unwrap_or_else(|| Duration::from_secs(DEFAULT_PIPELINE_FLUSH_SECS))
                    .as_secs(),
            ),
        };
        let target_poll = target_poll_for_flush(
            pipeline_flush
                .unwrap_or_else(|| Duration::from_secs(DEFAULT_PIPELINE_FLUSH_SECS))
                .as_secs(),
        );
        let cloudflare_api_token = args
            .cloudflare_api_token
            .or_else(|| env_var("IMPORT_CLOUDFLARE_API_TOKEN"))
            .or_else(|| env_var("CLOUDFLARE_API_TOKEN"));
        let target_checks_disabled = args.no_target_check
            || env_flag("IMPORT_DISABLE_TARGET_CHECKS")
            || env_var("IMPORT_TARGET_CHECKS").is_some_and(|value| {
                matches!(
                    value.to_ascii_lowercase().as_str(),
                    "0" | "false" | "no" | "off"
                )
            });
        let imports_anything = !args.skip_persons || !args.skip_groups || !args.skip_events;
        let require_target_check = !target_checks_disabled
            && (args.require_target_check || (!args.dry_run && imports_anything));
        let target_field_count = [
            target_account_id.is_some(),
            target_bucket.is_some(),
            target_auth_token.is_some(),
        ]
        .into_iter()
        .filter(|configured| *configured)
        .count();
        if target_field_count > 0 && target_field_count < 3 {
            return Err(ImportError::InvalidConfig(
                "R2 SQL target checks require IMPORT_TARGET_ACCOUNT_ID, IMPORT_TARGET_BUCKET, and IMPORT_TARGET_AUTH_TOKEN/WRANGLER_R2_SQL_AUTH_TOKEN".to_string(),
            ));
        }
        let target_checks_enabled = !target_checks_disabled && target_field_count == 3;
        if require_target_check && !target_checks_enabled {
            return Err(ImportError::InvalidConfig(
                "production imports require R2 SQL target checks by default; set IMPORT_TARGET_ACCOUNT_ID, IMPORT_TARGET_BUCKET, and IMPORT_TARGET_AUTH_TOKEN/WRANGLER_R2_SQL_AUTH_TOKEN, or pass --no-target-check to opt out".to_string(),
            ));
        }

        Ok(Self {
            posthog_host,
            posthog_project_id,
            posthog_environment_id,
            posthog_personal_api_key,
            pipeline_endpoint,
            pipeline_auth_token,
            persons_pipeline_endpoint,
            persons_pipeline_auth_token,
            pipeline_timeout,
            hogflare_api_key,
            posthog_team_id,
            posthog_group_types,
            batch_size,
            persons_offset,
            events_offset,
            max_persons,
            max_groups,
            max_events,
            events_after_timestamp,
            events_after_uuid,
            event_uuids_file,
            event_window_days,
            event_window_hours,
            from,
            to,
            import_persons: !args.skip_persons,
            import_groups: !args.skip_groups,
            import_events: !args.skip_events,
            emit_persons: !args.skip_person_output,
            dry_run: args.dry_run,
            import_state_file,
            target_account_id,
            target_bucket,
            target_table,
            persons_target_table,
            target_auth_token,
            target_wait,
            target_poll,
            target_checks_enabled,
            require_target_check,
            target_wait_explicit,
            pipeline_flush,
            cloudflare_api_token,
        })
    }

    fn events_after_cursor(&self) -> Option<EventCursor> {
        Some(EventCursor {
            timestamp: self.events_after_timestamp?,
            uuid: self.events_after_uuid.clone(),
        })
    }
}

#[derive(Debug, Default)]
struct ImportArgs {
    help: bool,
    posthog_host: Option<String>,
    posthog_project_id: Option<String>,
    posthog_environment_id: Option<String>,
    posthog_personal_api_key: Option<String>,
    pipeline_endpoint: Option<String>,
    pipeline_auth_token: Option<String>,
    persons_pipeline_endpoint: Option<String>,
    persons_pipeline_auth_token: Option<String>,
    pipeline_timeout_secs: Option<String>,
    hogflare_api_key: Option<String>,
    team_id: Option<String>,
    group_types: [Option<String>; 5],
    batch_size: Option<String>,
    persons_offset: Option<String>,
    events_offset: Option<String>,
    max_persons: Option<String>,
    max_groups: Option<String>,
    max_events: Option<String>,
    events_after_timestamp: Option<String>,
    events_after_uuid: Option<String>,
    event_uuids_file: Option<String>,
    event_window_days: Option<String>,
    event_window_hours: Option<String>,
    from: Option<String>,
    to: Option<String>,
    skip_persons: bool,
    skip_groups: bool,
    skip_events: bool,
    skip_person_output: bool,
    dry_run: bool,
    import_state_file: Option<String>,
    no_import_state: bool,
    target_account_id: Option<String>,
    target_bucket: Option<String>,
    target_table: Option<String>,
    persons_target_table: Option<String>,
    target_auth_token: Option<String>,
    target_wait_secs: Option<String>,
    pipeline_flush_secs: Option<String>,
    cloudflare_api_token: Option<String>,
    require_target_check: bool,
    no_target_check: bool,
}

impl ImportArgs {
    fn parse<I, S>(args: I) -> Result<Self, ImportError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut parsed = Self::default();
        let mut args = args.into_iter().map(Into::into);

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "-h" | "--help" => parsed.help = true,
                "--posthog-host" => parsed.posthog_host = Some(next_arg(&arg, &mut args)?),
                "--project-id" | "--posthog-project-id" => {
                    parsed.posthog_project_id = Some(next_arg(&arg, &mut args)?);
                }
                "--environment-id" | "--posthog-environment-id" => {
                    parsed.posthog_environment_id = Some(next_arg(&arg, &mut args)?);
                }
                "--personal-api-key" | "--posthog-personal-api-key" => {
                    parsed.posthog_personal_api_key = Some(next_arg(&arg, &mut args)?);
                }
                "--pipeline-endpoint" => {
                    parsed.pipeline_endpoint = Some(next_arg(&arg, &mut args)?)
                }
                "--pipeline-auth-token" => {
                    parsed.pipeline_auth_token = Some(next_arg(&arg, &mut args)?);
                }
                "--persons-pipeline-endpoint" => {
                    parsed.persons_pipeline_endpoint = Some(next_arg(&arg, &mut args)?);
                }
                "--persons-pipeline-auth-token" => {
                    parsed.persons_pipeline_auth_token = Some(next_arg(&arg, &mut args)?);
                }
                "--pipeline-timeout-secs" => {
                    parsed.pipeline_timeout_secs = Some(next_arg(&arg, &mut args)?);
                }
                "--hogflare-api-key" => parsed.hogflare_api_key = Some(next_arg(&arg, &mut args)?),
                "--team-id" => parsed.team_id = Some(next_arg(&arg, &mut args)?),
                "--group-type-0" => parsed.group_types[0] = Some(next_arg(&arg, &mut args)?),
                "--group-type-1" => parsed.group_types[1] = Some(next_arg(&arg, &mut args)?),
                "--group-type-2" => parsed.group_types[2] = Some(next_arg(&arg, &mut args)?),
                "--group-type-3" => parsed.group_types[3] = Some(next_arg(&arg, &mut args)?),
                "--group-type-4" => parsed.group_types[4] = Some(next_arg(&arg, &mut args)?),
                "--batch-size" => parsed.batch_size = Some(next_arg(&arg, &mut args)?),
                "--persons-offset" => parsed.persons_offset = Some(next_arg(&arg, &mut args)?),
                "--events-offset" => parsed.events_offset = Some(next_arg(&arg, &mut args)?),
                "--max-persons" => parsed.max_persons = Some(next_arg(&arg, &mut args)?),
                "--max-groups" => parsed.max_groups = Some(next_arg(&arg, &mut args)?),
                "--max-events" => parsed.max_events = Some(next_arg(&arg, &mut args)?),
                "--events-after-timestamp" => {
                    parsed.events_after_timestamp = Some(next_arg(&arg, &mut args)?);
                }
                "--events-after-uuid" => {
                    parsed.events_after_uuid = Some(next_arg(&arg, &mut args)?);
                }
                "--event-uuids-file" => {
                    parsed.event_uuids_file = Some(next_arg(&arg, &mut args)?);
                }
                "--event-window-days" => {
                    parsed.event_window_days = Some(next_arg(&arg, &mut args)?);
                }
                "--event-window-hours" => {
                    parsed.event_window_hours = Some(next_arg(&arg, &mut args)?);
                }
                "--from" => parsed.from = Some(next_arg(&arg, &mut args)?),
                "--to" => parsed.to = Some(next_arg(&arg, &mut args)?),
                "--skip-persons" => parsed.skip_persons = true,
                "--skip-groups" => parsed.skip_groups = true,
                "--skip-events" => parsed.skip_events = true,
                "--skip-person-output" => parsed.skip_person_output = true,
                "--dry-run" => parsed.dry_run = true,
                "--import-state-file" => {
                    parsed.import_state_file = Some(next_arg(&arg, &mut args)?);
                }
                "--no-import-state" => parsed.no_import_state = true,
                "--target-account-id" => {
                    parsed.target_account_id = Some(next_arg(&arg, &mut args)?);
                }
                "--target-bucket" => {
                    parsed.target_bucket = Some(next_arg(&arg, &mut args)?);
                }
                "--target-table" => {
                    parsed.target_table = Some(next_arg(&arg, &mut args)?);
                }
                "--persons-target-table" => {
                    parsed.persons_target_table = Some(next_arg(&arg, &mut args)?);
                }
                "--target-auth-token" => {
                    parsed.target_auth_token = Some(next_arg(&arg, &mut args)?);
                }
                "--target-wait-secs" => {
                    parsed.target_wait_secs = Some(next_arg(&arg, &mut args)?);
                }
                "--pipeline-flush-secs" => {
                    parsed.pipeline_flush_secs = Some(next_arg(&arg, &mut args)?);
                }
                "--cloudflare-api-token" => {
                    parsed.cloudflare_api_token = Some(next_arg(&arg, &mut args)?);
                }
                "--require-target-check" => parsed.require_target_check = true,
                "--no-target-check" => parsed.no_target_check = true,
                other => return Err(ImportError::UnknownArgument(other.to_string())),
            }
        }

        Ok(parsed)
    }
}

fn next_arg(flag: &str, args: &mut impl Iterator<Item = String>) -> Result<String, ImportError> {
    args.next()
        .filter(|value| !value.starts_with("--"))
        .ok_or_else(|| ImportError::MissingArgument(flag.to_string()))
}

fn env_var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

fn env_flag(name: &str) -> bool {
    env_var(name).is_some_and(|value| {
        matches!(
            value.to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

fn required(name: &'static str, value: Option<String>) -> Result<String, ImportError> {
    value.ok_or(ImportError::MissingConfig(name))
}

fn parse_url(name: &'static str, value: String) -> Result<Url, ImportError> {
    Url::parse(&value).map_err(|err| ImportError::InvalidUrl {
        name,
        value,
        message: err.to_string(),
    })
}

fn parse_target_table(name: &'static str, value: String) -> Result<String, ImportError> {
    let valid = value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.'));
    if valid && value.contains('.') {
        Ok(value)
    } else {
        Err(ImportError::InvalidConfig(format!(
            "{name} must be a namespace.table identifier"
        )))
    }
}

fn parse_usize(
    name: &'static str,
    value: Option<String>,
    default: usize,
) -> Result<usize, ImportError> {
    match value {
        Some(value) => value
            .parse::<usize>()
            .map_err(|err| ImportError::InvalidNumber {
                name,
                value,
                message: err.to_string(),
            }),
        None => Ok(default),
    }
}

fn parse_optional_usize(
    name: &'static str,
    value: Option<String>,
) -> Result<Option<usize>, ImportError> {
    match value {
        Some(value) => value
            .parse::<usize>()
            .map(Some)
            .map_err(|err| ImportError::InvalidNumber {
                name,
                value,
                message: err.to_string(),
            }),
        None => Ok(None),
    }
}

fn parse_duration_secs(
    name: &'static str,
    value: Option<String>,
    default: u64,
) -> Result<Duration, ImportError> {
    match value {
        Some(value) => value
            .parse::<u64>()
            .map(Duration::from_secs)
            .map_err(|err| ImportError::InvalidNumber {
                name,
                value,
                message: err.to_string(),
            }),
        None => Ok(Duration::from_secs(default)),
    }
}

fn parse_optional_duration_secs(
    name: &'static str,
    value: Option<String>,
) -> Result<Option<Duration>, ImportError> {
    match value {
        Some(value) => value
            .parse::<u64>()
            .map(Duration::from_secs)
            .map(Some)
            .map_err(|err| ImportError::InvalidNumber {
                name,
                value,
                message: err.to_string(),
            }),
        None => Ok(None),
    }
}

fn parse_optional_i64(
    name: &'static str,
    value: Option<String>,
) -> Result<Option<i64>, ImportError> {
    match value {
        Some(value) => value
            .parse::<i64>()
            .map(Some)
            .map_err(|err| ImportError::InvalidNumber {
                name,
                value,
                message: err.to_string(),
            }),
        None => Ok(None),
    }
}

fn parse_optional_datetime(
    name: &'static str,
    value: Option<String>,
) -> Result<Option<DateTime<Utc>>, ImportError> {
    let Some(value) = value else {
        return Ok(None);
    };

    parse_datetime(&value)
        .map(Some)
        .ok_or(ImportError::InvalidDateTime { name, value })
}

fn parse_datetime(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
        .or_else(|| {
            NaiveDate::parse_from_str(value, "%Y-%m-%d")
                .ok()
                .and_then(|date| date.and_hms_opt(0, 0, 0))
                .map(|dt| DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))
        })
}

fn deterministic_import_uuid(parts: &[&str]) -> String {
    deterministic_import_uuid_value(parts).to_string()
}

fn deterministic_import_uuid_value(parts: &[&str]) -> Uuid {
    let mut name = String::from("hogflare:posthog-import");
    for part in parts {
        name.push('\u{1f}');
        name.push_str(part);
    }
    Uuid::new_v5(&Uuid::NAMESPACE_URL, name.as_bytes())
}

fn stable_import_int_id(parts: &[&str]) -> i64 {
    let uuid = deterministic_import_uuid_value(parts);
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&uuid.as_bytes()[..8]);
    i64::from_be_bytes(bytes) & i64::MAX
}

fn target_wait_for_flush(flush_secs: u64) -> Duration {
    Duration::from_secs(
        (flush_secs.saturating_mul(2) + TARGET_WAIT_GRACE_SECS).max(MIN_TARGET_WAIT_SECS),
    )
}

fn target_poll_for_flush(flush_secs: u64) -> Duration {
    Duration::from_secs((flush_secs / 4).clamp(5, 30))
}

fn row_string(row: &Value, field: &str) -> Option<String> {
    match row {
        Value::Object(map) => map.get(field).and_then(value_to_string),
        _ => None,
    }
}

fn row_extra(row: &Value) -> Option<Map<String, Value>> {
    let extra = match row {
        Value::Object(map) => map.get("extra")?,
        _ => return None,
    };
    match extra {
        Value::Object(map) => Some(map.clone()),
        Value::String(raw) => serde_json::from_str::<Value>(raw)
            .ok()
            .and_then(|value| value.as_object().cloned()),
        _ => None,
    }
}

fn sql_string_list(values: &[String]) -> String {
    values
        .iter()
        .map(|value| sql_string_literal(value))
        .collect::<Vec<_>>()
        .join(", ")
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn target_table_parts(target_table: &str) -> (&str, &str) {
    target_table
        .rsplit_once('.')
        .unwrap_or(("default", target_table))
}

fn rolling_policy_flush_secs(rolling_policy: Option<&Value>) -> Option<u64> {
    let policy = rolling_policy?.as_object()?;
    let interval = policy.get("interval_seconds").and_then(Value::as_u64);
    let inactivity = policy.get("inactivity_seconds").and_then(Value::as_u64);
    interval.into_iter().chain(inactivity).max()
}

async fn align_target_wait_to_pipeline_flush(config: &mut ImportConfig) -> Result<(), ImportError> {
    if !config.target_checks_enabled || config.target_wait_explicit {
        return Ok(());
    }

    if let Some(flush) = config.pipeline_flush {
        config.target_wait = target_wait_for_flush(flush.as_secs());
        config.target_poll = target_poll_for_flush(flush.as_secs());
        return Ok(());
    }

    let Some(bucket) = config.target_bucket.clone() else {
        return Ok(());
    };
    let Some(client) = CloudflarePipelinesClient::from_config(config)? else {
        eprintln!(
            "PostHog import target checks: using conservative {}s wait; set IMPORT_PIPELINE_FLUSH_SECS or IMPORT_CLOUDFLARE_API_TOKEN to align with the Cloudflare Pipeline sink rolling policy",
            config.target_wait.as_secs()
        );
        return Ok(());
    };

    match client
        .target_sink_flush_secs(&bucket, &config.target_table)
        .await
    {
        Ok(Some(flush_secs)) => {
            config.pipeline_flush = Some(Duration::from_secs(flush_secs));
            config.target_wait = target_wait_for_flush(flush_secs);
            config.target_poll = target_poll_for_flush(flush_secs);
            eprintln!(
                "PostHog import target checks: aligned target wait to {}s for {}s pipeline flush",
                config.target_wait.as_secs(),
                flush_secs
            );
        }
        Ok(None) => {
            eprintln!(
                "PostHog import target checks: no matching Pipeline sink found; using conservative {}s wait",
                config.target_wait.as_secs()
            );
        }
        Err(err) => {
            eprintln!(
                "PostHog import target checks: could not read Pipeline sink rolling policy ({err}); using conservative {}s wait",
                config.target_wait.as_secs()
            );
        }
    }

    Ok(())
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ImportSummary {
    pub persons: usize,
    pub person_snapshots: usize,
    pub groups: usize,
    pub events: usize,
    pub skipped: usize,
    pub pipeline_batches: usize,
}

#[derive(Debug, Error)]
pub enum ImportError {
    #[error("{0}")]
    Usage(String),
    #[error("unknown argument `{0}`")]
    UnknownArgument(String),
    #[error("missing value for `{0}`")]
    MissingArgument(String),
    #[error("environment variable {0} is required")]
    MissingConfig(&'static str),
    #[error("invalid {name} URL `{value}`: {message}")]
    InvalidUrl {
        name: &'static str,
        value: String,
        message: String,
    },
    #[error("invalid {name} value `{value}`: {message}")]
    InvalidNumber {
        name: &'static str,
        value: String,
        message: String,
    },
    #[error("invalid {name} datetime `{value}`; use RFC3339 or YYYY-MM-DD")]
    InvalidDateTime { name: &'static str, value: String },
    #[error("invalid import config: {0}")]
    InvalidConfig(String),
    #[error("failed to read {path}: {source}")]
    ReadFile {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write {path}: {source}")]
    WriteFile {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to build HTTP client: {0}")]
    ClientBuild(#[source] reqwest::Error),
    #[error("PostHog request failed: {0}")]
    Transport(#[source] reqwest::Error),
    #[error("PostHog responded with {status} for {url}: {body}")]
    PostHogStatus {
        url: String,
        status: StatusCode,
        body: String,
    },
    #[error("invalid PostHog response: {0}")]
    InvalidPostHogResponse(String),
    #[error("R2 SQL target responded with {status}: {body}")]
    TargetStatus { status: StatusCode, body: String },
    #[error("R2 SQL target query failed: {0}")]
    TargetQuery(String),
    #[error(
        "pipeline send failed and target check confirmed only {confirmed} of {total} rows within {wait_secs}s; rerun later with target checks enabled before retrying: {source}"
    )]
    AmbiguousPipelineCommit {
        confirmed: usize,
        total: usize,
        wait_secs: u64,
        #[source]
        source: PipelineError,
    },
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Pipeline(#[from] PipelineError),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum ImportKind {
    Event,
    Person,
    PersonSnapshot,
    Group,
}

impl ImportKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Event => "event",
            Self::Person => "person",
            Self::PersonSnapshot => "person_snapshot",
            Self::Group => "group",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ImportKey {
    uuid: String,
    kind: ImportKind,
    logical_key: String,
    group_type: Option<String>,
}

impl ImportKey {
    fn event(uuid: String) -> Self {
        Self {
            logical_key: uuid.clone(),
            uuid,
            kind: ImportKind::Event,
            group_type: None,
        }
    }

    fn person(uuid: String, distinct_id: String) -> Self {
        Self {
            uuid,
            kind: ImportKind::Person,
            logical_key: distinct_id,
            group_type: None,
        }
    }

    fn person_snapshot(uuid: String, person_id: String) -> Self {
        Self {
            uuid,
            kind: ImportKind::PersonSnapshot,
            logical_key: person_id,
            group_type: None,
        }
    }

    fn group(uuid: String, group_type: String, group_key: String) -> Self {
        Self {
            uuid,
            kind: ImportKind::Group,
            logical_key: group_key,
            group_type: Some(group_type),
        }
    }

    fn state_key(&self) -> String {
        match self.kind {
            ImportKind::Event => format!("event\t{}", self.logical_key),
            ImportKind::Person => format!("person\t{}", self.logical_key),
            ImportKind::PersonSnapshot => format!("person_snapshot\t{}", self.logical_key),
            ImportKind::Group => format!(
                "group\t{}\t{}",
                self.group_type.as_deref().unwrap_or_default(),
                self.logical_key
            ),
        }
    }

    fn to_state_record(&self) -> ImportStateRecord {
        ImportStateRecord {
            kind: self.kind.as_str().to_string(),
            key: self.logical_key.clone(),
            uuid: Some(self.uuid.clone()),
            group_type: self.group_type.clone(),
        }
    }
}

#[derive(Debug, Clone)]
struct ImportBatchItem {
    key: ImportKey,
    event: PipelineEvent,
}

#[derive(Debug, Clone)]
struct PersonSnapshotBatchItem {
    key: ImportKey,
    record: PersonPipelineRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ImportStateRecord {
    kind: String,
    key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    uuid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    group_type: Option<String>,
}

impl ImportStateRecord {
    fn state_key(&self) -> Option<String> {
        match self.kind.as_str() {
            "event" => Some(format!("event\t{}", self.key)),
            "person" => Some(format!("person\t{}", self.key)),
            "person_snapshot" => Some(format!("person_snapshot\t{}", self.key)),
            "group" => Some(format!(
                "group\t{}\t{}",
                self.group_type.as_deref().unwrap_or_default(),
                self.key
            )),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
struct ImportState {
    path: Option<String>,
    seen: HashSet<String>,
}

impl ImportState {
    fn load(path: Option<String>) -> Result<Self, ImportError> {
        let Some(path_value) = path.clone() else {
            return Ok(Self {
                path,
                seen: HashSet::new(),
            });
        };
        let raw = match fs::read_to_string(&path_value) {
            Ok(raw) => raw,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(source) => {
                return Err(ImportError::ReadFile {
                    path: path_value,
                    source,
                });
            }
        };
        let mut seen = HashSet::new();
        for (index, line) in raw.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let record: ImportStateRecord = serde_json::from_str(line).map_err(|err| {
                ImportError::InvalidConfig(format!(
                    "invalid import state line {} in {}: {}",
                    index + 1,
                    path_value,
                    err
                ))
            })?;
            if let Some(key) = record.state_key() {
                seen.insert(key);
            }
        }

        Ok(Self { path, seen })
    }

    fn contains(&self, key: &ImportKey) -> bool {
        self.seen.contains(&key.state_key())
    }

    fn record(&mut self, keys: &[ImportKey]) -> Result<(), ImportError> {
        let new_keys = keys
            .iter()
            .filter(|key| !self.seen.contains(&key.state_key()))
            .collect::<Vec<_>>();
        if new_keys.is_empty() {
            return Ok(());
        }

        if let Some(path) = self.path.as_ref() {
            if let Some(parent) = Path::new(path).parent() {
                if !parent.as_os_str().is_empty() {
                    fs::create_dir_all(parent).map_err(|source| ImportError::WriteFile {
                        path: path.clone(),
                        source,
                    })?;
                }
            }
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .map_err(|source| ImportError::WriteFile {
                    path: path.clone(),
                    source,
                })?;
            for key in &new_keys {
                let line = serde_json::to_string(&key.to_state_record())?;
                writeln!(file, "{line}").map_err(|source| ImportError::WriteFile {
                    path: path.clone(),
                    source,
                })?;
            }
        }

        for key in new_keys {
            self.seen.insert(key.state_key());
        }
        Ok(())
    }
}

#[derive(Clone)]
struct R2SqlTarget {
    client: Client,
    endpoint: Url,
    auth_token: String,
    table: String,
}

impl R2SqlTarget {
    fn from_config(config: &ImportConfig) -> Result<Option<Self>, ImportError> {
        Self::from_config_table(config, config.target_table.clone())
    }

    fn from_config_table(
        config: &ImportConfig,
        table: String,
    ) -> Result<Option<Self>, ImportError> {
        if !config.target_checks_enabled {
            return Ok(None);
        }
        let (Some(account_id), Some(bucket), Some(auth_token)) = (
            config.target_account_id.clone(),
            config.target_bucket.clone(),
            config.target_auth_token.clone(),
        ) else {
            return Ok(None);
        };
        let endpoint = parse_url(
            "IMPORT_TARGET",
            format!(
                "https://api.sql.cloudflarestorage.com/api/v1/accounts/{account_id}/r2-sql/query/{bucket}"
            ),
        )?;
        let client = Client::builder()
            .timeout(config.pipeline_timeout)
            .build()
            .map_err(ImportError::ClientBuild)?;
        Ok(Some(Self {
            client,
            endpoint,
            auth_token,
            table,
        }))
    }

    async fn existing_keys(&self, keys: &[ImportKey]) -> Result<HashSet<String>, ImportError> {
        let mut existing = HashSet::new();
        self.mark_existing_by_uuid(keys, &mut existing).await?;
        self.mark_existing_persons(keys, &mut existing).await?;
        self.mark_existing_groups(keys, &mut existing).await?;
        Ok(existing)
    }

    async fn existing_person_snapshots(
        &self,
        keys: &[ImportKey],
    ) -> Result<HashSet<String>, ImportError> {
        let mut existing = HashSet::new();
        self.mark_existing_person_snapshots(keys, &mut existing)
            .await?;
        Ok(existing)
    }

    async fn wait_for_existing_keys(
        &self,
        keys: &[ImportKey],
        wait: Duration,
        poll: Duration,
    ) -> Result<HashSet<String>, ImportError> {
        let started = Instant::now();
        loop {
            let existing = self.existing_keys(keys).await?;
            if existing.len() == keys.len() || started.elapsed() >= wait {
                return Ok(existing);
            }

            let remaining = wait.saturating_sub(started.elapsed());
            let sleep_for = remaining.min(poll);
            if sleep_for.is_zero() {
                return Ok(existing);
            }
            tokio::time::sleep(sleep_for).await;
        }
    }

    async fn wait_for_person_snapshots(
        &self,
        keys: &[ImportKey],
        wait: Duration,
        poll: Duration,
    ) -> Result<HashSet<String>, ImportError> {
        let started = Instant::now();
        loop {
            let existing = self.existing_person_snapshots(keys).await?;
            if existing.len() == keys.len() || started.elapsed() >= wait {
                return Ok(existing);
            }

            let remaining = wait.saturating_sub(started.elapsed());
            let sleep_for = remaining.min(poll);
            if sleep_for.is_zero() {
                return Ok(existing);
            }
            tokio::time::sleep(sleep_for).await;
        }
    }

    async fn mark_existing_by_uuid(
        &self,
        keys: &[ImportKey],
        existing: &mut HashSet<String>,
    ) -> Result<(), ImportError> {
        let mut keys_by_uuid: HashMap<String, Vec<String>> = HashMap::new();
        for key in keys {
            keys_by_uuid
                .entry(key.uuid.clone())
                .or_default()
                .push(key.state_key());
        }
        let uuids = keys_by_uuid.keys().cloned().collect::<Vec<_>>();
        for chunk in uuids.chunks(500) {
            let uuid_list = sql_string_list(chunk);
            let query = format!(
                "select uuid from {} where source = 'posthog' and extra like '%\"hogflare_import\":true%' and uuid in ({uuid_list}) group by uuid limit {}",
                self.table,
                chunk.len()
            );
            for row in self.query_rows(query).await? {
                if let Some(uuid) = row_string(&row, "uuid") {
                    if let Some(state_keys) = keys_by_uuid.get(&uuid) {
                        existing.extend(state_keys.iter().cloned());
                    }
                }
            }
        }
        Ok(())
    }

    async fn mark_existing_persons(
        &self,
        keys: &[ImportKey],
        existing: &mut HashSet<String>,
    ) -> Result<(), ImportError> {
        let mut keys_by_distinct_id: HashMap<String, String> = HashMap::new();
        for key in keys
            .iter()
            .filter(|key| matches!(key.kind, ImportKind::Person))
        {
            keys_by_distinct_id.insert(key.logical_key.clone(), key.state_key());
        }
        let distinct_ids = keys_by_distinct_id.keys().cloned().collect::<Vec<_>>();
        for chunk in distinct_ids.chunks(500) {
            let distinct_id_list = sql_string_list(chunk);
            let query = format!(
                "select distinct_id from {} where source = 'posthog' and event = '$identify' and extra like '%\"hogflare_import_kind\":\"person\"%' and distinct_id in ({distinct_id_list}) group by distinct_id limit {}",
                self.table,
                chunk.len()
            );
            for row in self.query_rows(query).await? {
                if let Some(distinct_id) = row_string(&row, "distinct_id") {
                    if let Some(state_key) = keys_by_distinct_id.get(&distinct_id) {
                        existing.insert(state_key.clone());
                    }
                }
            }
        }
        Ok(())
    }

    async fn mark_existing_groups(
        &self,
        keys: &[ImportKey],
        existing: &mut HashSet<String>,
    ) -> Result<(), ImportError> {
        let mut keys_by_group_key: HashMap<String, Vec<&ImportKey>> = HashMap::new();
        for key in keys
            .iter()
            .filter(|key| matches!(key.kind, ImportKind::Group))
        {
            keys_by_group_key
                .entry(key.logical_key.clone())
                .or_default()
                .push(key);
        }
        let group_keys = keys_by_group_key.keys().cloned().collect::<Vec<_>>();
        for chunk in group_keys.chunks(500) {
            let group_key_list = sql_string_list(chunk);
            let query = format!(
                "select distinct_id, extra from {} where source = 'posthog' and event = '$groupidentify' and extra like '%\"hogflare_import_kind\":\"group\"%' and distinct_id in ({group_key_list}) group by distinct_id, extra limit 5000",
                self.table
            );
            for row in self.query_rows(query).await? {
                let Some(group_key) = row_string(&row, "distinct_id") else {
                    continue;
                };
                let Some(extra) = row_extra(&row) else {
                    continue;
                };
                let Some(candidates) = keys_by_group_key.get(&group_key) else {
                    continue;
                };
                for key in candidates {
                    if extra.get("group_type").and_then(Value::as_str) == key.group_type.as_deref()
                        && extra.get("group_key").and_then(Value::as_str)
                            == Some(key.logical_key.as_str())
                    {
                        existing.insert(key.state_key());
                    }
                }
            }
        }
        Ok(())
    }

    async fn mark_existing_person_snapshots(
        &self,
        keys: &[ImportKey],
        existing: &mut HashSet<String>,
    ) -> Result<(), ImportError> {
        let mut keys_by_person_id: HashMap<String, String> = HashMap::new();
        for key in keys
            .iter()
            .filter(|key| matches!(key.kind, ImportKind::PersonSnapshot))
        {
            keys_by_person_id.insert(key.logical_key.clone(), key.state_key());
        }
        let person_ids = keys_by_person_id.keys().cloned().collect::<Vec<_>>();
        for chunk in person_ids.chunks(500) {
            let person_id_list = sql_string_list(chunk);
            let query = format!(
                "select person_id from {} where source = 'posthog' and person_id in ({person_id_list}) group by person_id limit {}",
                self.table,
                chunk.len()
            );
            for row in self.query_rows(query).await? {
                if let Some(person_id) = row_string(&row, "person_id") {
                    if let Some(state_key) = keys_by_person_id.get(&person_id) {
                        existing.insert(state_key.clone());
                    }
                }
            }
        }
        Ok(())
    }

    async fn query_rows(&self, query: String) -> Result<Vec<Value>, ImportError> {
        let response = self
            .client
            .post(self.endpoint.clone())
            .bearer_auth(&self.auth_token)
            .json(&json!({ "query": query }))
            .send()
            .await
            .map_err(ImportError::Transport)?;
        let status = response.status();
        let body = response.text().await.map_err(ImportError::Transport)?;
        if !status.is_success() {
            return Err(ImportError::TargetStatus { status, body });
        }

        let response: R2SqlResponse = serde_json::from_str(&body)?;
        if !response.success {
            let messages = response
                .errors
                .into_iter()
                .map(|err| err.message)
                .chain(response.messages.into_iter())
                .collect::<Vec<_>>()
                .join("; ");
            return Err(ImportError::TargetQuery(messages));
        }
        Ok(response
            .result
            .map(|result| result.rows)
            .unwrap_or_default())
    }
}

#[derive(Debug, Deserialize)]
struct R2SqlResponse {
    success: bool,
    result: Option<R2SqlResult>,
    #[serde(default)]
    errors: Vec<R2SqlError>,
    #[serde(default)]
    messages: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct R2SqlResult {
    #[serde(default)]
    rows: Vec<Value>,
}

#[derive(Debug, Deserialize)]
struct R2SqlError {
    message: String,
}

#[derive(Clone)]
struct CloudflarePipelinesClient {
    client: Client,
    account_id: String,
    auth_token: String,
}

impl CloudflarePipelinesClient {
    fn from_config(config: &ImportConfig) -> Result<Option<Self>, ImportError> {
        let (Some(account_id), Some(auth_token)) = (
            config.target_account_id.clone(),
            config.cloudflare_api_token.clone(),
        ) else {
            return Ok(None);
        };
        let client = Client::builder()
            .timeout(config.pipeline_timeout)
            .build()
            .map_err(ImportError::ClientBuild)?;
        Ok(Some(Self {
            client,
            account_id,
            auth_token,
        }))
    }

    async fn target_sink_flush_secs(
        &self,
        bucket: &str,
        target_table: &str,
    ) -> Result<Option<u64>, ImportError> {
        let (namespace, table_name) = target_table_parts(target_table);
        let sinks = self
            .get_result_array(&format!(
                "https://api.cloudflare.com/client/v4/accounts/{}/pipelines/v1/sinks",
                self.account_id
            ))
            .await?;
        let mut flush_secs: Option<u64> = None;

        for sink in sinks {
            let Some(config) = sink.get("config") else {
                continue;
            };
            if config.get("bucket").and_then(Value::as_str) != Some(bucket) {
                continue;
            }
            if config.get("table_name").and_then(Value::as_str) != Some(table_name) {
                continue;
            }
            let sink_namespace = config
                .get("namespace")
                .and_then(Value::as_str)
                .unwrap_or("default");
            if sink_namespace != namespace {
                continue;
            }
            if let Some(candidate) = rolling_policy_flush_secs(config.get("rolling_policy")) {
                flush_secs = Some(flush_secs.map_or(candidate, |current| current.max(candidate)));
            }
        }

        Ok(flush_secs)
    }

    async fn get_result_array(&self, url: &str) -> Result<Vec<Value>, ImportError> {
        let response = self
            .client
            .get(url)
            .bearer_auth(&self.auth_token)
            .send()
            .await
            .map_err(ImportError::Transport)?;
        let status = response.status();
        let body = response.text().await.map_err(ImportError::Transport)?;
        if !status.is_success() {
            return Err(ImportError::TargetStatus { status, body });
        }

        let response: CloudflareApiResponse = serde_json::from_str(&body)?;
        if !response.success {
            let messages = response
                .errors
                .into_iter()
                .map(|err| err.message)
                .chain(response.messages.into_iter())
                .collect::<Vec<_>>()
                .join("; ");
            return Err(ImportError::TargetQuery(messages));
        }

        Ok(response
            .result
            .and_then(|result| result.as_array().cloned())
            .unwrap_or_default())
    }
}

#[derive(Debug, Deserialize)]
struct CloudflareApiResponse {
    success: bool,
    result: Option<Value>,
    #[serde(default)]
    errors: Vec<CloudflareApiError>,
    #[serde(default)]
    messages: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct CloudflareApiError {
    message: String,
}

pub async fn run_import(config: ImportConfig) -> Result<ImportSummary, ImportError> {
    let mut config = config;
    align_target_wait_to_pipeline_flush(&mut config).await?;
    let client = PostHogClient::new(
        config.posthog_host.clone(),
        config.posthog_project_id.clone(),
        config.posthog_environment_id.clone(),
        config.posthog_personal_api_key.clone(),
        config.pipeline_timeout,
    )?;
    let pipeline = PipelineClient::new_without_retries(
        config.pipeline_endpoint.clone(),
        config.pipeline_auth_token.clone(),
        config.pipeline_timeout,
    )?;
    let persons_pipeline = match config.persons_pipeline_endpoint.clone() {
        Some(endpoint) => Some(PipelineClient::new_without_retries(
            endpoint,
            config.persons_pipeline_auth_token.clone(),
            config.pipeline_timeout,
        )?),
        None => None,
    };
    let mut importer = Importer::new(config, client, pipeline, persons_pipeline)?;

    importer.run().await
}

struct Importer {
    config: ImportConfig,
    posthog: PostHogClient,
    pipeline: PipelineClient,
    persons_pipeline: Option<PipelineClient>,
    group_type_map: GroupTypeMap,
    persons_by_distinct_id: HashMap<String, ImportedPersonSnapshot>,
    group_properties: HashMap<(String, String), Map<String, Value>>,
    import_state: ImportState,
    target: Option<R2SqlTarget>,
    persons_target: Option<R2SqlTarget>,
    summary: ImportSummary,
}

impl Importer {
    fn new(
        config: ImportConfig,
        posthog: PostHogClient,
        pipeline: PipelineClient,
        persons_pipeline: Option<PipelineClient>,
    ) -> Result<Self, ImportError> {
        let group_type_map = GroupTypeMap::new(config.posthog_group_types.clone());
        let import_state = ImportState::load(config.import_state_file.clone())?;
        let target = R2SqlTarget::from_config(&config)?;
        let persons_target = if persons_pipeline.is_some() {
            R2SqlTarget::from_config_table(&config, config.persons_target_table.clone())?
        } else {
            None
        };
        Ok(Self {
            config,
            posthog,
            pipeline,
            persons_pipeline,
            group_type_map,
            persons_by_distinct_id: HashMap::new(),
            group_properties: HashMap::new(),
            import_state,
            target,
            persons_target,
            summary: ImportSummary::default(),
        })
    }

    async fn run(&mut self) -> Result<ImportSummary, ImportError> {
        if self.config.import_persons {
            self.import_persons().await?;
        }

        if self.config.import_groups {
            self.import_groups().await?;
        }

        if self.config.import_events {
            self.import_events().await?;
        }

        Ok(self.summary.clone())
    }

    async fn import_persons(&mut self) -> Result<(), ImportError> {
        let mut event_buffer = Vec::new();
        let mut snapshot_buffer = Vec::new();
        let mut next = self
            .next_limit(self.config.max_persons, self.summary.persons)
            .map(|limit| self.posthog.persons_url(limit, self.config.persons_offset))
            .transpose()?;

        while let Some(url) = next {
            let (persons, page_next) = self.posthog.get_page::<PostHogPerson>(url).await?;
            next = page_next;

            for person in persons {
                if let Some(snapshot) = person.snapshot() {
                    for distinct_id in &person.distinct_ids {
                        self.persons_by_distinct_id
                            .insert(distinct_id.clone(), snapshot.clone());
                    }
                }

                if self.config.emit_persons {
                    if let Some(event) = person.to_pipeline_event(&self.config) {
                        event_buffer.push(ImportBatchItem {
                            key: ImportKey::person(event.uuid.clone(), event.distinct_id.clone()),
                            event,
                        });
                    }
                }

                if self.persons_pipeline.is_some() {
                    if let Some(record) = person.to_person_pipeline_record(&self.config) {
                        snapshot_buffer.push(PersonSnapshotBatchItem {
                            key: ImportKey::person_snapshot(
                                record.uuid.clone(),
                                record.person_id.clone(),
                            ),
                            record,
                        });
                    }
                }

                if event_buffer.len() >= self.config.batch_size {
                    self.send(std::mem::take(&mut event_buffer)).await?;
                }
                if snapshot_buffer.len() >= self.config.batch_size {
                    let sent = self
                        .send_person_snapshots(std::mem::take(&mut snapshot_buffer))
                        .await?;
                    self.summary.person_snapshots += sent;
                }

                self.summary.persons += 1;

                if self
                    .config
                    .max_persons
                    .is_some_and(|max_persons| self.summary.persons >= max_persons)
                {
                    next = None;
                    break;
                }
            }
        }

        self.send(event_buffer).await?;
        let sent = self.send_person_snapshots(snapshot_buffer).await?;
        self.summary.person_snapshots += sent;
        Ok(())
    }

    async fn import_groups(&mut self) -> Result<(), ImportError> {
        let group_types = self.posthog.list_group_types().await?;
        let mut buffer = Vec::new();

        for group_type in group_types {
            let mut next = self
                .next_limit(self.config.max_groups, self.summary.groups)
                .map(|limit| self.posthog.groups_url(group_type.index, limit))
                .transpose()?;
            while let Some(url) = next {
                let (groups, page_next) = self.posthog.get_page::<PostHogGroup>(url).await?;
                next = page_next;

                for group in groups {
                    if let Some(properties) = group.properties_map() {
                        self.group_properties.insert(
                            (group_type.name.clone(), group.group_key.clone()),
                            properties,
                        );
                    }

                    let event =
                        group.to_pipeline_event(&self.config, &self.group_type_map, &group_type);
                    buffer.push(ImportBatchItem {
                        key: ImportKey::group(
                            event.uuid.clone(),
                            group_type.name.clone(),
                            group.group_key.clone(),
                        ),
                        event,
                    });

                    if buffer.len() >= self.config.batch_size {
                        self.send(std::mem::take(&mut buffer)).await?;
                    }

                    self.summary.groups += 1;

                    if self
                        .config
                        .max_groups
                        .is_some_and(|max_groups| self.summary.groups >= max_groups)
                    {
                        next = None;
                        break;
                    }
                }
            }

            if self
                .config
                .max_groups
                .is_some_and(|max_groups| self.summary.groups >= max_groups)
            {
                break;
            }
        }

        self.send(buffer).await.map(|_| ())
    }

    async fn import_events(&mut self) -> Result<(), ImportError> {
        if let Some(path) = self.config.event_uuids_file.clone() {
            return self.import_events_by_uuid_file(&path).await;
        }

        if let Some(window_days) = self.config.event_window_days {
            return self
                .import_events_windowed(ChronoDuration::days(window_days))
                .await;
        }
        if let Some(window_hours) = self.config.event_window_hours {
            return self
                .import_events_windowed(ChronoDuration::hours(window_hours))
                .await;
        }

        self.import_events_range(
            self.config.from,
            self.config.to,
            self.config.events_after_cursor(),
            self.config.events_offset,
        )
        .await
    }

    async fn import_events_by_uuid_file(&mut self, path: &str) -> Result<(), ImportError> {
        let raw = fs::read_to_string(path).map_err(|source| ImportError::ReadFile {
            path: path.to_string(),
            source,
        })?;
        let mut seen = HashSet::new();
        let uuids = raw
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .filter(|uuid| seen.insert((*uuid).to_string()))
            .map(ToString::to_string)
            .collect::<Vec<_>>();

        for chunk in uuids.chunks(self.config.batch_size) {
            let rows = self.posthog.query_events_by_uuids(chunk).await?;
            let mut rows_seen = HashSet::new();
            let mut events = Vec::with_capacity(rows.len());

            for row in rows {
                let row = EventRow::from_value(row)?;
                if let Some(uuid) = row.uuid.as_ref() {
                    if !rows_seen.insert(uuid.clone()) {
                        continue;
                    }
                }
                if let Some(event) = self.row_to_pipeline_event(row)? {
                    events.push(ImportBatchItem {
                        key: ImportKey::event(event.uuid.clone()),
                        event,
                    });
                }
            }

            let previous_events = self.summary.events;
            let sent = self.send(events).await?;
            self.summary.events += sent;
            if previous_events / PROGRESS_INTERVAL < self.summary.events / PROGRESS_INTERVAL {
                eprintln!("PostHog import progress: events={}", self.summary.events);
            }
        }

        Ok(())
    }

    async fn import_events_windowed(
        &mut self,
        window_duration: ChronoDuration,
    ) -> Result<(), ImportError> {
        let Some(mut window_start) = self.config.from.or(self.config.events_after_timestamp) else {
            return Err(ImportError::InvalidConfig(
                "event window imports require IMPORT_FROM or IMPORT_EVENTS_AFTER_TIMESTAMP"
                    .to_string(),
            ));
        };
        let import_end = self.config.to.unwrap_or_else(Utc::now);
        let mut cursor = self.config.events_after_cursor();

        while window_start < import_end {
            let window_end = (window_start + window_duration).min(import_end);
            self.import_events_range(Some(window_start), Some(window_end), cursor.take(), 0)
                .await?;
            window_start = window_end;
        }

        Ok(())
    }

    async fn import_events_range(
        &mut self,
        from: Option<DateTime<Utc>>,
        to: Option<DateTime<Utc>>,
        initial_cursor: Option<EventCursor>,
        initial_offset: usize,
    ) -> Result<(), ImportError> {
        let mut offset = initial_offset;
        let mut cursor = initial_cursor;
        loop {
            let Some(limit) = self.next_limit(self.config.max_events, self.summary.events) else {
                break;
            };
            let rows = self
                .posthog
                .query_events(limit, offset, from, to, cursor.as_ref())
                .await?;
            if rows.is_empty() {
                break;
            }

            let count = rows.len();
            let mut events = Vec::with_capacity(count);
            let mut next_cursor = None;
            for row in rows {
                let row = EventRow::from_value(row)?;
                next_cursor = row.cursor();
                if let Some(event) = self.row_to_pipeline_event(row)? {
                    events.push(ImportBatchItem {
                        key: ImportKey::event(event.uuid.clone()),
                        event,
                    });
                }
            }

            let next_cursor = validated_event_page_cursor(count, limit, next_cursor)?;

            let previous_events = self.summary.events;
            let sent = self.send(events).await?;
            self.summary.events += sent;
            if previous_events / PROGRESS_INTERVAL < self.summary.events / PROGRESS_INTERVAL {
                eprintln!("PostHog import progress: events={}", self.summary.events);
            }

            if count < limit {
                break;
            }
            cursor = next_cursor;
            offset = 0;
        }

        Ok(())
    }

    fn next_limit(&self, max_items: Option<usize>, imported: usize) -> Option<usize> {
        match max_items {
            Some(max_items) => max_items
                .checked_sub(imported)
                .filter(|remaining| *remaining > 0)
                .map(|remaining| remaining.min(self.config.batch_size)),
            None => Some(self.config.batch_size),
        }
    }

    fn row_to_pipeline_event(&self, row: EventRow) -> Result<Option<PipelineEvent>, ImportError> {
        let Some(event_name) = row.event else {
            return Ok(None);
        };
        let Some(distinct_id) = row.distinct_id.filter(|id| !id.is_empty()) else {
            return Ok(None);
        };

        let properties = row.properties;
        let timestamp_key = row.timestamp.map(|value| value.to_rfc3339());
        let created_at_key = row.created_at.map(|value| value.to_rfc3339());
        let properties_key = properties.as_ref().map(Value::to_string);
        let event_uuid = row.uuid.clone().unwrap_or_else(|| {
            deterministic_import_uuid(&[
                &self.config.posthog_project_id,
                self.config.posthog_environment_id.as_deref().unwrap_or(""),
                "event",
                &event_name,
                &distinct_id,
                timestamp_key.as_deref().unwrap_or(""),
                created_at_key.as_deref().unwrap_or(""),
                properties_key.as_deref().unwrap_or(""),
            ])
        });
        let groups = extract_groups_from_value(properties.as_ref());
        let (group_slots, group_properties) = self.group_fields(groups.as_ref());
        let person = self.persons_by_distinct_id.get(&distinct_id);
        let mut extra = HashMap::new();
        extra.insert("hogflare_import".to_string(), Value::Bool(true));
        extra.insert(
            "hogflare_import_source".to_string(),
            Value::String("posthog".to_string()),
        );
        if let Some(created_at) = row.created_at {
            extra.insert(
                "posthog_created_at".to_string(),
                Value::String(created_at.to_rfc3339()),
            );
        }
        if let Some(uuid) = row.uuid.as_ref() {
            extra.insert("posthog_uuid".to_string(), Value::String(uuid.clone()));
        } else {
            extra.insert(
                "hogflare_import_generated_uuid".to_string(),
                Value::Bool(true),
            );
        }

        Ok(Some(PipelineEvent {
            uuid: event_uuid,
            team_id: self.config.posthog_team_id,
            source: "posthog",
            event: event_name,
            distinct_id,
            created_at: row.created_at.unwrap_or_else(Utc::now),
            timestamp: row.timestamp,
            properties,
            context: None,
            person_id: person.and_then(|snapshot| snapshot.person_id.clone()),
            person_created_at: person.and_then(|snapshot| snapshot.created_at),
            person_properties: person.and_then(|snapshot| snapshot.properties.clone()),
            group0: group_slots[0].clone(),
            group1: group_slots[1].clone(),
            group2: group_slots[2].clone(),
            group3: group_slots[3].clone(),
            group4: group_slots[4].clone(),
            group_properties,
            api_key: self.config.hogflare_api_key.clone(),
            extra,
        }))
    }

    fn group_fields(
        &self,
        groups: Option<&Map<String, Value>>,
    ) -> ([Option<String>; 5], Option<Value>) {
        let mut slots = [None, None, None, None, None];
        let mut props = Map::new();

        let Some(groups) = groups else {
            return (slots, None);
        };

        for (group_type, group_key) in groups {
            let Some(group_key) = value_to_string(group_key) else {
                continue;
            };
            if let Some(index) = self.group_type_map.index_for(group_type) {
                slots[index] = Some(group_key.clone());
            }
            if let Some(group_props) = self
                .group_properties
                .get(&(group_type.clone(), group_key.clone()))
            {
                props.insert(group_type.clone(), Value::Object(group_props.clone()));
            }
        }

        let group_properties = if props.is_empty() {
            None
        } else {
            Some(Value::Object(props))
        };
        (slots, group_properties)
    }

    async fn send(&mut self, items: Vec<ImportBatchItem>) -> Result<usize, ImportError> {
        let items = self.filter_existing(items).await?;
        if items.is_empty() {
            return Ok(0);
        }

        let keys = items
            .iter()
            .map(|item| item.key.clone())
            .collect::<Vec<_>>();
        if self.config.dry_run {
            self.summary.pipeline_batches += 1;
            return Ok(items.len());
        }

        let events = items
            .iter()
            .map(|item| item.event.clone())
            .collect::<Vec<_>>();
        match self.pipeline.send(events).await {
            Ok(()) => {
                self.import_state.record(&keys)?;
                self.summary.pipeline_batches += 1;
                Ok(keys.len())
            }
            Err(source) => self.handle_ambiguous_send(keys, source).await,
        }
    }

    async fn send_person_snapshots(
        &mut self,
        items: Vec<PersonSnapshotBatchItem>,
    ) -> Result<usize, ImportError> {
        let items = self.filter_existing_person_snapshots(items).await?;
        if items.is_empty() {
            return Ok(0);
        }

        let keys = items
            .iter()
            .map(|item| item.key.clone())
            .collect::<Vec<_>>();
        if self.config.dry_run {
            self.summary.pipeline_batches += 1;
            return Ok(items.len());
        }

        let records = items
            .iter()
            .map(|item| item.record.clone())
            .collect::<Vec<_>>();
        let Some(pipeline) = self.persons_pipeline.as_ref() else {
            return Ok(0);
        };
        match pipeline.send_records(records).await {
            Ok(()) => {
                self.import_state.record(&keys)?;
                self.summary.pipeline_batches += 1;
                Ok(keys.len())
            }
            Err(source) => {
                self.handle_ambiguous_person_snapshot_send(keys, source)
                    .await
            }
        }
    }

    async fn filter_existing(
        &mut self,
        items: Vec<ImportBatchItem>,
    ) -> Result<Vec<ImportBatchItem>, ImportError> {
        let mut batch_seen = HashSet::new();
        let mut filtered = Vec::with_capacity(items.len());
        let mut skipped = 0;

        for item in items {
            let state_key = item.key.state_key();
            if self.import_state.contains(&item.key) || !batch_seen.insert(state_key) {
                skipped += 1;
            } else {
                filtered.push(item);
            }
        }

        if let Some(target) = self.target.as_ref() {
            let keys = filtered
                .iter()
                .map(|item| item.key.clone())
                .collect::<Vec<_>>();
            let existing = target.existing_keys(&keys).await?;
            if !existing.is_empty() {
                let mut existing_keys = Vec::new();
                filtered.retain(|item| {
                    if existing.contains(&item.key.state_key()) {
                        existing_keys.push(item.key.clone());
                        false
                    } else {
                        true
                    }
                });
                skipped += existing_keys.len();
                if !self.config.dry_run {
                    self.import_state.record(&existing_keys)?;
                }
            }
        }

        self.summary.skipped += skipped;
        Ok(filtered)
    }

    async fn filter_existing_person_snapshots(
        &mut self,
        items: Vec<PersonSnapshotBatchItem>,
    ) -> Result<Vec<PersonSnapshotBatchItem>, ImportError> {
        let mut batch_seen = HashSet::new();
        let mut filtered = Vec::with_capacity(items.len());
        let mut skipped = 0;

        for item in items {
            let state_key = item.key.state_key();
            if self.import_state.contains(&item.key) || !batch_seen.insert(state_key) {
                skipped += 1;
            } else {
                filtered.push(item);
            }
        }

        if let Some(target) = self.persons_target.as_ref() {
            let keys = filtered
                .iter()
                .map(|item| item.key.clone())
                .collect::<Vec<_>>();
            let existing = target.existing_person_snapshots(&keys).await?;
            if !existing.is_empty() {
                let mut existing_keys = Vec::new();
                filtered.retain(|item| {
                    if existing.contains(&item.key.state_key()) {
                        existing_keys.push(item.key.clone());
                        false
                    } else {
                        true
                    }
                });
                skipped += existing_keys.len();
                if !self.config.dry_run {
                    self.import_state.record(&existing_keys)?;
                }
            }
        }

        self.summary.skipped += skipped;
        Ok(filtered)
    }

    async fn handle_ambiguous_send(
        &mut self,
        keys: Vec<ImportKey>,
        source: PipelineError,
    ) -> Result<usize, ImportError> {
        let Some(target) = self.target.as_ref() else {
            return Err(ImportError::AmbiguousPipelineCommit {
                confirmed: 0,
                total: keys.len(),
                wait_secs: 0,
                source,
            });
        };

        let existing = target
            .wait_for_existing_keys(&keys, self.config.target_wait, self.config.target_poll)
            .await?;
        let confirmed = keys
            .iter()
            .filter(|key| existing.contains(&key.state_key()))
            .cloned()
            .collect::<Vec<_>>();
        if !confirmed.is_empty() {
            self.import_state.record(&confirmed)?;
        }
        if confirmed.len() == keys.len() {
            self.summary.pipeline_batches += 1;
            return Ok(confirmed.len());
        }

        Err(ImportError::AmbiguousPipelineCommit {
            confirmed: confirmed.len(),
            total: keys.len(),
            wait_secs: self.config.target_wait.as_secs(),
            source,
        })
    }

    async fn handle_ambiguous_person_snapshot_send(
        &mut self,
        keys: Vec<ImportKey>,
        source: PipelineError,
    ) -> Result<usize, ImportError> {
        let Some(target) = self.persons_target.as_ref() else {
            return Err(ImportError::AmbiguousPipelineCommit {
                confirmed: 0,
                total: keys.len(),
                wait_secs: 0,
                source,
            });
        };

        let existing = target
            .wait_for_person_snapshots(&keys, self.config.target_wait, self.config.target_poll)
            .await?;
        let confirmed = keys
            .iter()
            .filter(|key| existing.contains(&key.state_key()))
            .cloned()
            .collect::<Vec<_>>();
        if !confirmed.is_empty() {
            self.import_state.record(&confirmed)?;
        }
        if confirmed.len() == keys.len() {
            self.summary.pipeline_batches += 1;
            return Ok(confirmed.len());
        }

        Err(ImportError::AmbiguousPipelineCommit {
            confirmed: confirmed.len(),
            total: keys.len(),
            wait_secs: self.config.target_wait.as_secs(),
            source,
        })
    }
}

#[derive(Clone)]
struct PostHogClient {
    base: Url,
    project_id: String,
    environment_id: Option<String>,
    personal_api_key: String,
    client: Client,
}

impl PostHogClient {
    fn new(
        base: Url,
        project_id: String,
        environment_id: Option<String>,
        personal_api_key: String,
        timeout: Duration,
    ) -> Result<Self, ImportError> {
        let client = Client::builder()
            .timeout(timeout)
            .build()
            .map_err(ImportError::ClientBuild)?;
        Ok(Self {
            base,
            project_id,
            environment_id,
            personal_api_key,
            client,
        })
    }

    fn persons_url(&self, limit: usize, offset: usize) -> Result<Url, ImportError> {
        self.resource_url(&format!("persons/?limit={limit}&offset={offset}"))
    }

    async fn list_group_types(&self) -> Result<Vec<PostHogGroupType>, ImportError> {
        let url = self.project_url("groups_types/")?;
        let value = self.get_json(url).await?;
        page_items(&value)
            .into_iter()
            .map(serde_json::from_value)
            .collect::<Result<Vec<_>, _>>()
            .map_err(ImportError::Json)
    }

    fn groups_url(&self, group_type_index: i64, limit: usize) -> Result<Url, ImportError> {
        self.resource_url(&format!(
            "groups/?group_type_index={group_type_index}&limit={limit}&offset=0"
        ))
    }

    async fn query_events(
        &self,
        limit: usize,
        offset: usize,
        from: Option<DateTime<Utc>>,
        to: Option<DateTime<Utc>>,
        cursor: Option<&EventCursor>,
    ) -> Result<Vec<Value>, ImportError> {
        let query = events_query(limit, offset, from, to, cursor);
        let url = self.resource_url("query/")?;
        let value = self
            .post_json(
                url,
                &json!({
                    "query": {
                        "kind": "HogQLQuery",
                        "query": query,
                    },
                    "name": "hogflare historical event import",
                }),
            )
            .await?;

        value
            .get("results")
            .and_then(Value::as_array)
            .cloned()
            .ok_or_else(|| {
                ImportError::InvalidPostHogResponse(
                    "query response did not contain a results array".to_string(),
                )
            })
    }

    async fn query_events_by_uuids(&self, uuids: &[String]) -> Result<Vec<Value>, ImportError> {
        if uuids.is_empty() {
            return Ok(Vec::new());
        }

        let query = events_by_uuids_query(uuids);
        let url = self.resource_url("query/")?;
        let value = self
            .post_json(
                url,
                &json!({
                    "query": {
                        "kind": "HogQLQuery",
                        "query": query,
                    },
                    "name": "hogflare targeted event import",
                }),
            )
            .await?;

        value
            .get("results")
            .and_then(Value::as_array)
            .cloned()
            .ok_or_else(|| {
                ImportError::InvalidPostHogResponse(
                    "query response did not contain a results array".to_string(),
                )
            })
    }

    async fn get_page<T>(&self, url: Url) -> Result<(Vec<T>, Option<Url>), ImportError>
    where
        T: for<'de> Deserialize<'de>,
    {
        let value = self.get_json(url).await?;
        let items = page_items(&value)
            .into_iter()
            .map(serde_json::from_value)
            .collect::<Result<Vec<T>, _>>()?;
        let next = page_next(&value)?;
        Ok((items, next))
    }

    async fn get_json(&self, url: Url) -> Result<Value, ImportError> {
        for attempt in 0..=POSTHOG_MAX_RETRIES {
            match self
                .client
                .get(url.clone())
                .bearer_auth(&self.personal_api_key)
                .send()
                .await
            {
                Ok(response) => {
                    let status = response.status();
                    let body = response.text().await.map_err(ImportError::Transport)?;
                    if retryable_posthog_status(status) && attempt < POSTHOG_MAX_RETRIES {
                        sleep_before_retry(attempt).await;
                        continue;
                    }
                    return parse_response_body(url, status, body);
                }
                Err(err) if attempt < POSTHOG_MAX_RETRIES => {
                    eprintln!("PostHog request transport error, retrying: {err}");
                    sleep_before_retry(attempt).await;
                }
                Err(err) => return Err(ImportError::Transport(err)),
            }
        }

        unreachable!("retry loop always returns")
    }

    async fn post_json(&self, url: Url, body: &Value) -> Result<Value, ImportError> {
        for attempt in 0..=POSTHOG_MAX_RETRIES {
            match self
                .client
                .post(url.clone())
                .bearer_auth(&self.personal_api_key)
                .json(body)
                .send()
                .await
            {
                Ok(response) => {
                    let status = response.status();
                    let body = response.text().await.map_err(ImportError::Transport)?;
                    if retryable_posthog_status(status) && attempt < POSTHOG_MAX_RETRIES {
                        sleep_before_retry(attempt).await;
                        continue;
                    }
                    return parse_response_body(url, status, body);
                }
                Err(err) if attempt < POSTHOG_MAX_RETRIES => {
                    eprintln!("PostHog request transport error, retrying: {err}");
                    sleep_before_retry(attempt).await;
                }
                Err(err) => return Err(ImportError::Transport(err)),
            }
        }

        unreachable!("retry loop always returns")
    }

    fn project_url(&self, path: &str) -> Result<Url, ImportError> {
        self.base
            .join(&format!("api/projects/{}/{path}", self.project_id))
            .map_err(|err| ImportError::InvalidUrl {
                name: "POSTHOG_HOST",
                value: self.base.to_string(),
                message: err.to_string(),
            })
    }

    fn resource_url(&self, path: &str) -> Result<Url, ImportError> {
        if let Some(environment_id) = self.environment_id.as_ref() {
            self.base
                .join(&format!("api/environments/{environment_id}/{path}"))
                .map_err(|err| ImportError::InvalidUrl {
                    name: "POSTHOG_HOST",
                    value: self.base.to_string(),
                    message: err.to_string(),
                })
        } else {
            self.project_url(path)
        }
    }
}

fn parse_response_body(url: Url, status: StatusCode, body: String) -> Result<Value, ImportError> {
    if !status.is_success() {
        return Err(ImportError::PostHogStatus {
            url: url.to_string(),
            status,
            body,
        });
    }

    serde_json::from_str(&body).map_err(ImportError::Json)
}

fn retryable_posthog_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::BAD_GATEWAY | StatusCode::SERVICE_UNAVAILABLE | StatusCode::TOO_MANY_REQUESTS
    )
}

async fn sleep_before_retry(attempt: usize) {
    let seconds = 2_u64.pow(attempt as u32);
    tokio::time::sleep(Duration::from_secs(seconds)).await;
}

fn page_items(value: &Value) -> Vec<Value> {
    if let Some(results) = value.get("results").and_then(Value::as_array) {
        return results.clone();
    }
    if let Some(array) = value.as_array() {
        return array.clone();
    }
    if value.is_object() {
        return vec![value.clone()];
    }
    Vec::new()
}

fn page_next(value: &Value) -> Result<Option<Url>, ImportError> {
    match value.get("next") {
        Some(Value::String(next)) if !next.is_empty() => {
            Url::parse(next)
                .map(Some)
                .map_err(|err| ImportError::InvalidUrl {
                    name: "PostHog next",
                    value: next.clone(),
                    message: err.to_string(),
                })
        }
        _ => Ok(None),
    }
}

fn events_query(
    limit: usize,
    offset: usize,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
    cursor: Option<&EventCursor>,
) -> String {
    let mut filters = Vec::new();
    if let Some(from) = from {
        filters.push(format!("timestamp >= {}", hogql_datetime_literal(from)));
    }
    if let Some(to) = to {
        filters.push(format!("timestamp < {}", hogql_datetime_literal(to)));
    }
    if let Some(cursor) = cursor {
        let timestamp = hogql_datetime_literal(cursor.timestamp);
        let cursor_filter = match cursor.uuid.as_ref() {
            Some(uuid) => format!(
                "(timestamp > {timestamp} or (timestamp = {timestamp} and toString(uuid) > {}))",
                hogql_string_literal(uuid)
            ),
            None => format!("timestamp > {timestamp}"),
        };
        filters.push(cursor_filter);
    }

    let where_clause = if filters.is_empty() {
        String::new()
    } else {
        format!("where {}", filters.join(" and "))
    };

    let offset_clause = if offset == 0 {
        String::new()
    } else {
        format!(" offset {offset}")
    };

    format!(
        "select uuid, event, toString(distinct_id), timestamp, created_at, properties \
         from events {where_clause} order by timestamp asc, toString(uuid) asc limit {limit}{offset_clause}"
    )
}

fn validated_event_page_cursor(
    count: usize,
    limit: usize,
    cursor: Option<EventCursor>,
) -> Result<Option<EventCursor>, ImportError> {
    if count == limit && cursor.is_none() {
        return Err(ImportError::InvalidPostHogResponse(
            "cannot safely paginate a full events page because its final row is missing a timestamp or UUID tie-breaker"
                .to_string(),
        ));
    }
    Ok(cursor)
}

fn events_by_uuids_query(uuids: &[String]) -> String {
    let uuid_list = uuids
        .iter()
        .map(|uuid| hogql_string_literal(uuid))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "select uuid, event, toString(distinct_id), timestamp, created_at, properties \
         from events where toString(uuid) in ({uuid_list}) order by timestamp asc, toString(uuid) asc"
    )
}

fn hogql_datetime_literal(value: DateTime<Utc>) -> String {
    format!(
        "toDateTime64('{}', 6, 'UTC')",
        value.format("%Y-%m-%d %H:%M:%S%.6f")
    )
}

fn hogql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

#[derive(Debug, Clone, Deserialize)]
struct PostHogPerson {
    #[serde(default)]
    id: Option<Value>,
    #[serde(default)]
    uuid: Option<String>,
    #[serde(default)]
    distinct_ids: Vec<String>,
    #[serde(default)]
    properties: Option<Value>,
    #[serde(default)]
    created_at: Option<DateTime<Utc>>,
}

impl PostHogPerson {
    fn primary_distinct_id(&self) -> Option<String> {
        self.distinct_ids
            .iter()
            .find(|id| !id.is_empty())
            .cloned()
            .or_else(|| self.uuid.clone())
            .or_else(|| self.id.as_ref().and_then(value_to_string))
    }

    fn person_key(&self) -> Option<String> {
        self.posthog_person_id()
            .or_else(|| self.primary_distinct_id())
    }

    fn posthog_person_id(&self) -> Option<String> {
        self.uuid
            .clone()
            .or_else(|| self.id.as_ref().and_then(value_to_string))
    }

    fn person_int_id(&self) -> Option<i64> {
        self.id.as_ref().and_then(value_to_i64)
    }

    fn snapshot(&self) -> Option<ImportedPersonSnapshot> {
        Some(ImportedPersonSnapshot {
            person_id: self.person_key(),
            created_at: self.created_at,
            properties: self.properties.clone(),
        })
    }

    fn to_pipeline_event(&self, config: &ImportConfig) -> Option<PipelineEvent> {
        let distinct_id = self.primary_distinct_id()?;
        let person_id = self.posthog_person_id();
        let person_key = person_id.clone().unwrap_or_else(|| distinct_id.clone());
        let import_uuid = deterministic_import_uuid(&[
            &config.posthog_project_id,
            config.posthog_environment_id.as_deref().unwrap_or(""),
            "person",
            &person_key,
        ]);
        let mut extra = HashMap::new();
        extra.insert("hogflare_import".to_string(), Value::Bool(true));
        extra.insert(
            "hogflare_import_kind".to_string(),
            Value::String("person".to_string()),
        );
        if let Some(id) = self.id.as_ref() {
            extra.insert("posthog_person_id".to_string(), id.clone());
        }
        if let Some(uuid) = self.uuid.as_ref() {
            extra.insert(
                "posthog_person_uuid".to_string(),
                Value::String(uuid.clone()),
            );
        }
        extra.insert(
            "posthog_distinct_ids".to_string(),
            Value::Array(
                self.distinct_ids
                    .iter()
                    .map(|id| Value::String(id.clone()))
                    .collect(),
            ),
        );

        Some(PipelineEvent {
            uuid: import_uuid,
            team_id: config.posthog_team_id,
            source: "posthog",
            event: "$identify".to_string(),
            distinct_id,
            created_at: self.created_at.unwrap_or_else(Utc::now),
            timestamp: self.created_at,
            properties: None,
            context: None,
            person_id,
            person_created_at: self.created_at,
            person_properties: self.properties.clone(),
            group0: None,
            group1: None,
            group2: None,
            group3: None,
            group4: None,
            group_properties: None,
            api_key: config.hogflare_api_key.clone(),
            extra,
        })
    }

    fn to_person_pipeline_record(&self, config: &ImportConfig) -> Option<PersonPipelineRecord> {
        let canonical_distinct_id = self.primary_distinct_id()?;
        let person_id = self.person_key()?;
        let created_at = self.created_at.unwrap_or_else(Utc::now);
        let source_event_uuid = deterministic_import_uuid(&[
            &config.posthog_project_id,
            config.posthog_environment_id.as_deref().unwrap_or(""),
            "person",
            &person_id,
        ]);
        let uuid = deterministic_import_uuid(&[
            &config.posthog_project_id,
            config.posthog_environment_id.as_deref().unwrap_or(""),
            "person_snapshot",
            &person_id,
        ]);
        let distinct_ids = if self.distinct_ids.is_empty() {
            vec![canonical_distinct_id.clone()]
        } else {
            self.distinct_ids.clone()
        };
        let properties = self
            .properties
            .clone()
            .unwrap_or_else(|| Value::Object(Map::new()));
        let person_int_id = self.person_int_id().unwrap_or_else(|| {
            stable_import_int_id(&[
                &config.posthog_project_id,
                config.posthog_environment_id.as_deref().unwrap_or(""),
                "person",
                &person_id,
            ])
        });

        Some(PersonPipelineRecord {
            uuid,
            team_id: config.posthog_team_id,
            source: "posthog",
            operation: "import".to_string(),
            person_id,
            person_int_id,
            canonical_distinct_id,
            distinct_ids: Value::Array(distinct_ids.into_iter().map(Value::String).collect()),
            created_at,
            updated_at: Utc::now(),
            version: 1,
            properties: properties.clone(),
            properties_set_once: Value::Object(Map::new()),
            merged_properties: properties,
            api_key: config.hogflare_api_key.clone(),
            source_event_uuid,
        })
    }
}

#[derive(Debug, Clone)]
struct ImportedPersonSnapshot {
    person_id: Option<String>,
    created_at: Option<DateTime<Utc>>,
    properties: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
struct PostHogGroupType {
    #[serde(rename = "group_type")]
    name: String,
    #[serde(rename = "group_type_index")]
    index: i64,
}

#[derive(Debug, Clone, Deserialize)]
struct PostHogGroup {
    group_key: String,
    #[serde(default)]
    group_properties: Option<Value>,
    #[serde(default)]
    created_at: Option<DateTime<Utc>>,
}

impl PostHogGroup {
    fn properties_map(&self) -> Option<Map<String, Value>> {
        self.group_properties
            .as_ref()
            .and_then(Value::as_object)
            .cloned()
    }

    fn to_pipeline_event(
        &self,
        config: &ImportConfig,
        group_type_map: &GroupTypeMap,
        group_type: &PostHogGroupType,
    ) -> PipelineEvent {
        let mut slots = [None, None, None, None, None];
        if let Some(index) = group_type_map.index_for(&group_type.name) {
            slots[index] = Some(self.group_key.clone());
        }

        let mut extra = HashMap::new();
        extra.insert("hogflare_import".to_string(), Value::Bool(true));
        extra.insert(
            "hogflare_import_kind".to_string(),
            Value::String("group".to_string()),
        );
        extra.insert(
            "group_type".to_string(),
            Value::String(group_type.name.clone()),
        );
        extra.insert(
            "group_key".to_string(),
            Value::String(self.group_key.clone()),
        );
        extra.insert(
            "posthog_group_type_index".to_string(),
            Value::Number(group_type.index.into()),
        );

        let mut group_properties = Map::new();
        if let Some(properties) = self.properties_map() {
            group_properties.insert(group_type.name.clone(), Value::Object(properties));
        }
        let group_properties = if group_properties.is_empty() {
            None
        } else {
            Some(Value::Object(group_properties))
        };

        let import_uuid = deterministic_import_uuid(&[
            &config.posthog_project_id,
            config.posthog_environment_id.as_deref().unwrap_or(""),
            "group",
            &group_type.name,
            &self.group_key,
        ]);

        PipelineEvent {
            uuid: import_uuid,
            team_id: config.posthog_team_id,
            source: "posthog",
            event: "$groupidentify".to_string(),
            distinct_id: self.group_key.clone(),
            created_at: self.created_at.unwrap_or_else(Utc::now),
            timestamp: self.created_at,
            properties: self.group_properties.clone(),
            context: None,
            person_id: None,
            person_created_at: None,
            person_properties: None,
            group0: slots[0].clone(),
            group1: slots[1].clone(),
            group2: slots[2].clone(),
            group3: slots[3].clone(),
            group4: slots[4].clone(),
            group_properties,
            api_key: config.hogflare_api_key.clone(),
            extra,
        }
    }
}

#[derive(Debug, Clone)]
struct EventCursor {
    timestamp: DateTime<Utc>,
    uuid: Option<String>,
}

#[derive(Debug)]
struct EventRow {
    uuid: Option<String>,
    event: Option<String>,
    distinct_id: Option<String>,
    timestamp: Option<DateTime<Utc>>,
    created_at: Option<DateTime<Utc>>,
    properties: Option<Value>,
}

impl EventRow {
    fn from_value(value: Value) -> Result<Self, ImportError> {
        match value {
            Value::Array(values) => Ok(Self {
                uuid: values.first().and_then(value_to_string),
                event: values.get(1).and_then(value_to_string),
                distinct_id: values.get(2).and_then(value_to_string),
                timestamp: values.get(3).and_then(value_to_datetime),
                created_at: values.get(4).and_then(value_to_datetime),
                properties: values.get(5).and_then(normalize_json_value),
            }),
            Value::Object(map) => Ok(Self {
                uuid: map.get("uuid").and_then(value_to_string),
                event: map.get("event").and_then(value_to_string),
                distinct_id: map.get("distinct_id").and_then(value_to_string),
                timestamp: map.get("timestamp").and_then(value_to_datetime),
                created_at: map.get("created_at").and_then(value_to_datetime),
                properties: map.get("properties").and_then(normalize_json_value),
            }),
            other => Err(ImportError::InvalidPostHogResponse(format!(
                "event row must be an array or object, got {other:?}"
            ))),
        }
    }

    fn cursor(&self) -> Option<EventCursor> {
        let uuid = self.uuid.as_ref().filter(|uuid| !uuid.is_empty())?.clone();
        Some(EventCursor {
            timestamp: self.timestamp?,
            uuid: Some(uuid),
        })
    }
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

fn value_to_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(value) => value.as_i64(),
        Value::String(value) => value.parse().ok(),
        _ => None,
    }
}

fn value_to_datetime(value: &Value) -> Option<DateTime<Utc>> {
    value.as_str().and_then(parse_datetime)
}

fn normalize_json_value(value: &Value) -> Option<Value> {
    match value {
        Value::String(raw) => serde_json::from_str(raw)
            .ok()
            .or_else(|| Some(value.clone())),
        Value::Null => None,
        other => Some(other.clone()),
    }
}

fn extract_groups_from_value(value: Option<&Value>) -> Option<Map<String, Value>> {
    value?.as_object()?.get("$groups")?.as_object().cloned()
}

fn usage() -> String {
    [
        "Usage: cargo run --bin import_posthog -- [options]",
        "",
        "Required via flags or env:",
        "  --project-id / POSTHOG_PROJECT_ID",
        "  --personal-api-key / POSTHOG_PERSONAL_API_KEY",
        "  --pipeline-endpoint / CLOUDFLARE_PIPELINE_ENDPOINT",
        "",
        "Common options:",
        "  --posthog-host https://us.posthog.com",
        "  --environment-id <environment id>",
        "  --pipeline-auth-token <token>",
        "  --persons-pipeline-endpoint <url>",
        "  --persons-pipeline-auth-token <token>",
        "  --hogflare-api-key <project API key>",
        "  --from 2025-01-01",
        "  --to 2025-02-01",
        "  --batch-size 500",
        "  --persons-offset 0 | --events-offset 0",
        "  --events-after-timestamp 2024-09-21T03:24:11Z",
        "  --events-after-uuid 0192129b-c354-77b4-b496-9be7ec571fb4",
        "  --event-uuids-file /tmp/missing-event-uuids.txt",
        "  --event-window-days 7",
        "  --event-window-hours 6",
        "  --max-persons 1000 | --max-groups 1000 | --max-events 1000",
        "  --import-state-file .hogflare-import-state.jsonl | --no-import-state",
        "  --target-account-id <cloudflare account id>",
        "  --target-bucket <r2 bucket>",
        "  --target-table default.hogflare_events_v3",
        "  --persons-target-table default.hogflare_persons_v2",
        "  --target-auth-token <r2 sql token>",
        "  --target-wait-secs <secs>",
        "  --pipeline-flush-secs 300",
        "  --cloudflare-api-token <token with Pipelines read>",
        "  --require-target-check",
        "  --no-target-check",
        "  --skip-persons | --skip-groups | --skip-events",
        "  --skip-person-output",
        "  --dry-run",
    ]
    .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_config() -> ImportConfig {
        ImportConfig {
            posthog_host: Url::parse("https://us.posthog.com").unwrap(),
            posthog_project_id: "123".to_string(),
            posthog_environment_id: Some("456".to_string()),
            posthog_personal_api_key: "phx_test".to_string(),
            pipeline_endpoint: Url::parse("http://127.0.0.1:1/").unwrap(),
            pipeline_auth_token: None,
            persons_pipeline_endpoint: None,
            persons_pipeline_auth_token: None,
            pipeline_timeout: Duration::from_secs(1),
            hogflare_api_key: Some("phc_test".to_string()),
            posthog_team_id: Some(42),
            posthog_group_types: [Some("company".to_string()), None, None, None, None],
            batch_size: 100,
            persons_offset: 0,
            events_offset: 0,
            max_persons: None,
            max_groups: None,
            max_events: None,
            events_after_timestamp: None,
            events_after_uuid: None,
            event_uuids_file: None,
            event_window_days: None,
            event_window_hours: None,
            from: None,
            to: None,
            import_persons: true,
            import_groups: true,
            import_events: true,
            emit_persons: true,
            dry_run: true,
            import_state_file: None,
            target_account_id: None,
            target_bucket: None,
            target_table: DEFAULT_TARGET_TABLE.to_string(),
            persons_target_table: DEFAULT_PERSONS_TARGET_TABLE.to_string(),
            target_auth_token: None,
            target_wait: target_wait_for_flush(DEFAULT_PIPELINE_FLUSH_SECS),
            target_poll: target_poll_for_flush(DEFAULT_PIPELINE_FLUSH_SECS),
            target_checks_enabled: false,
            require_target_check: false,
            target_wait_explicit: false,
            pipeline_flush: None,
            cloudflare_api_token: None,
        }
    }

    #[test]
    fn builds_bounded_events_query() {
        let from = parse_datetime("2025-01-01").unwrap();
        let to = parse_datetime("2025-02-01T12:00:00Z").unwrap();
        let query = events_query(250, 500, Some(from), Some(to), None);

        assert!(query.contains("timestamp >= toDateTime64('2025-01-01 00:00:00.000000', 6, 'UTC')"));
        assert!(query.contains("timestamp < toDateTime64('2025-02-01 12:00:00.000000', 6, 'UTC')"));
        assert!(query.contains("order by timestamp asc, toString(uuid) asc"));
        assert!(query.contains("limit 250 offset 500"));
    }

    #[test]
    fn builds_keyset_events_query() {
        let cursor = EventCursor {
            timestamp: parse_datetime("2024-09-21T03:24:11Z").unwrap(),
            uuid: Some("0192129b-c354-77b4-b496-9be7ec571fb4".to_string()),
        };
        let query = events_query(200, 0, None, None, Some(&cursor));

        assert!(query.contains("timestamp > toDateTime64('2024-09-21 03:24:11.000000', 6, 'UTC')"));
        assert!(query.contains("toString(uuid) > '0192129b-c354-77b4-b496-9be7ec571fb4'"));
        assert!(query.ends_with("limit 200"));
        assert!(!query.contains(" offset "));
    }

    #[test]
    fn omits_zero_offset_from_initial_events_query() {
        let query = events_query(100, 0, None, None, None);

        assert!(query.ends_with("limit 100"));
        assert!(!query.contains(" offset "));
    }

    #[test]
    fn event_cursor_requires_uuid_tie_breaker() {
        let timestamp = parse_datetime("2025-01-01T12:00:00Z").unwrap();
        let without_uuid = EventRow {
            uuid: None,
            event: Some("test".to_string()),
            distinct_id: Some("user-1".to_string()),
            timestamp: Some(timestamp),
            created_at: Some(timestamp),
            properties: None,
        };
        let with_uuid = EventRow {
            uuid: Some("event-1".to_string()),
            event: Some("test".to_string()),
            distinct_id: Some("user-1".to_string()),
            timestamp: Some(timestamp),
            created_at: Some(timestamp),
            properties: None,
        };

        assert!(without_uuid.cursor().is_none());
        assert_eq!(with_uuid.cursor().unwrap().uuid.as_deref(), Some("event-1"));
    }

    #[test]
    fn full_events_page_requires_safe_cursor() {
        let error = validated_event_page_cursor(100, 100, None).unwrap_err();

        assert!(matches!(error, ImportError::InvalidPostHogResponse(_)));
        assert!(validated_event_page_cursor(99, 100, None)
            .unwrap()
            .is_none());
    }

    #[test]
    fn parses_import_caps_from_args() {
        let config = ImportConfig::from_env_and_args([
            "--project-id",
            "123",
            "--personal-api-key",
            "phx_test",
            "--pipeline-endpoint",
            "http://127.0.0.1:1/",
            "--persons-pipeline-endpoint",
            "http://127.0.0.1:2/",
            "--persons-pipeline-auth-token",
            "persons-token",
            "--dry-run",
            "--persons-offset",
            "10",
            "--events-offset",
            "20",
            "--max-persons",
            "1",
            "--max-groups",
            "2",
            "--max-events",
            "3",
            "--events-after-timestamp",
            "2024-09-21T03:24:11Z",
            "--events-after-uuid",
            "0192129b-c354-77b4-b496-9be7ec571fb4",
            "--event-uuids-file",
            "/tmp/missing.txt",
            "--event-window-days",
            "7",
            "--event-window-hours",
            "6",
            "--skip-person-output",
        ])
        .unwrap_err();

        assert!(matches!(config, ImportError::InvalidConfig(_)));

        let config = ImportConfig::from_env_and_args([
            "--project-id",
            "123",
            "--personal-api-key",
            "phx_test",
            "--pipeline-endpoint",
            "http://127.0.0.1:1/",
            "--persons-pipeline-endpoint",
            "http://127.0.0.1:2/",
            "--persons-pipeline-auth-token",
            "persons-token",
            "--persons-target-table",
            "default.custom_persons",
            "--dry-run",
            "--persons-offset",
            "10",
            "--events-offset",
            "20",
            "--max-persons",
            "1",
            "--max-groups",
            "2",
            "--max-events",
            "3",
            "--events-after-timestamp",
            "2024-09-21T03:24:11Z",
            "--events-after-uuid",
            "0192129b-c354-77b4-b496-9be7ec571fb4",
            "--event-uuids-file",
            "/tmp/missing.txt",
            "--event-window-hours",
            "6",
            "--skip-person-output",
        ])
        .unwrap();

        assert_eq!(config.persons_offset, 10);
        assert_eq!(config.events_offset, 20);
        assert_eq!(config.max_persons, Some(1));
        assert_eq!(config.max_groups, Some(2));
        assert_eq!(config.max_events, Some(3));
        assert_eq!(
            config.events_after_timestamp,
            parse_datetime("2024-09-21T03:24:11Z")
        );
        assert_eq!(
            config.events_after_uuid.as_deref(),
            Some("0192129b-c354-77b4-b496-9be7ec571fb4")
        );
        assert_eq!(
            config.persons_pipeline_endpoint.as_ref().map(Url::as_str),
            Some("http://127.0.0.1:2/")
        );
        assert_eq!(
            config.persons_pipeline_auth_token.as_deref(),
            Some("persons-token")
        );
        assert_eq!(config.persons_target_table, "default.custom_persons");
        assert_eq!(config.event_uuids_file.as_deref(), Some("/tmp/missing.txt"));
        assert_eq!(config.event_window_days, None);
        assert_eq!(config.event_window_hours, Some(6));
        assert!(!config.emit_persons);
        assert_eq!(
            config.import_state_file.as_deref(),
            Some(DEFAULT_IMPORT_STATE_FILE)
        );
        assert!(!config.target_checks_enabled);
    }

    #[test]
    fn real_import_requires_target_checks_by_default() {
        let err = ImportConfig::from_env_and_args([
            "--project-id",
            "123",
            "--personal-api-key",
            "phx_test",
            "--pipeline-endpoint",
            "http://127.0.0.1:1/",
        ])
        .unwrap_err();
        assert!(matches!(err, ImportError::InvalidConfig(_)));

        let config = ImportConfig::from_env_and_args([
            "--project-id",
            "123",
            "--personal-api-key",
            "phx_test",
            "--pipeline-endpoint",
            "http://127.0.0.1:1/",
            "--no-target-check",
        ])
        .unwrap();
        assert!(!config.target_checks_enabled);
        assert!(!config.require_target_check);
    }

    #[test]
    fn aligns_target_wait_with_pipeline_flush() {
        let config = ImportConfig::from_env_and_args([
            "--project-id",
            "123",
            "--personal-api-key",
            "phx_test",
            "--pipeline-endpoint",
            "http://127.0.0.1:1/",
            "--target-account-id",
            "account",
            "--target-bucket",
            "bucket",
            "--target-auth-token",
            "token",
            "--pipeline-flush-secs",
            "120",
        ])
        .unwrap();

        assert!(config.target_checks_enabled);
        assert!(config.require_target_check);
        assert_eq!(config.target_wait, Duration::from_secs(270));
        assert_eq!(config.target_poll, Duration::from_secs(30));
    }

    #[test]
    fn builds_targeted_uuid_events_query() {
        let query = events_by_uuids_query(&[
            "019e24be-606a-7bd2-b048-80c3b0c1d3c6".to_string(),
            "quote-'slash-\\".to_string(),
        ]);

        assert!(query.contains(
            "toString(uuid) in ('019e24be-606a-7bd2-b048-80c3b0c1d3c6', 'quote-\\'slash-\\\\')"
        ));
        assert!(query.contains("order by timestamp asc, toString(uuid) asc"));
    }

    #[test]
    fn person_import_event_preserves_person_snapshot() {
        let config = base_config();
        let person: PostHogPerson = serde_json::from_value(json!({
            "id": 7,
            "uuid": "person-uuid",
            "distinct_ids": ["user-1", "anon-1"],
            "properties": {"email": "u@example.com"},
            "created_at": "2025-01-02T03:04:05Z"
        }))
        .unwrap();

        let event = person.to_pipeline_event(&config).unwrap();
        assert_eq!(event.event, "$identify");
        assert_eq!(event.distinct_id, "user-1");
        assert_eq!(event.person_id.as_deref(), Some("person-uuid"));
        assert_eq!(
            event.person_properties,
            Some(json!({"email": "u@example.com"}))
        );
        assert_eq!(event.extra["posthog_person_id"], json!(7));
        assert_eq!(event.api_key.as_deref(), Some("phc_test"));
    }

    #[test]
    fn person_import_event_uses_stable_uuid() {
        let config = base_config();
        let person: PostHogPerson = serde_json::from_value(json!({
            "uuid": "person-uuid",
            "distinct_ids": ["user-1"]
        }))
        .unwrap();

        let first = person.to_pipeline_event(&config).unwrap();
        let second = person.to_pipeline_event(&config).unwrap();

        assert_eq!(first.uuid, second.uuid);
    }

    #[test]
    fn person_import_snapshot_uses_stable_uuid_and_schema() {
        let config = base_config();
        let person: PostHogPerson = serde_json::from_value(json!({
            "id": 7,
            "uuid": "person-uuid",
            "distinct_ids": ["user-1", "anon-1"],
            "properties": {"email": "u@example.com"},
            "created_at": "2025-01-02T03:04:05Z"
        }))
        .unwrap();

        let event = person.to_pipeline_event(&config).unwrap();
        let first = person.to_person_pipeline_record(&config).unwrap();
        let second = person.to_person_pipeline_record(&config).unwrap();

        assert_eq!(first.uuid, second.uuid);
        assert_eq!(first.source, "posthog");
        assert_eq!(first.operation, "import");
        assert_eq!(first.person_id, "person-uuid");
        assert_eq!(first.person_int_id, 7);
        assert_eq!(first.canonical_distinct_id, "user-1");
        assert_eq!(first.distinct_ids, json!(["user-1", "anon-1"]));
        assert_eq!(
            first.created_at,
            parse_datetime("2025-01-02T03:04:05Z").unwrap()
        );
        assert_eq!(first.version, 1);
        assert_eq!(first.properties, json!({"email": "u@example.com"}));
        assert_eq!(first.properties_set_once, json!({}));
        assert_eq!(first.merged_properties, json!({"email": "u@example.com"}));
        assert_eq!(first.api_key.as_deref(), Some("phc_test"));
        assert_eq!(first.source_event_uuid, event.uuid);
    }

    #[test]
    fn client_prefers_environment_scoped_person_and_group_urls() {
        let config = base_config();
        let client = PostHogClient::new(
            config.posthog_host.clone(),
            config.posthog_project_id.clone(),
            config.posthog_environment_id.clone(),
            config.posthog_personal_api_key.clone(),
            config.pipeline_timeout,
        )
        .unwrap();

        assert_eq!(
            client.persons_url(50, 100).unwrap().as_str(),
            "https://us.posthog.com/api/environments/456/persons/?limit=50&offset=100"
        );
        assert_eq!(
            client.groups_url(0, 50).unwrap().as_str(),
            "https://us.posthog.com/api/environments/456/groups/?group_type_index=0&limit=50&offset=0"
        );
    }

    #[test]
    fn group_import_event_uses_configured_group_slot() {
        let config = base_config();
        let group_type = PostHogGroupType {
            name: "company".to_string(),
            index: 0,
        };
        let group: PostHogGroup = serde_json::from_value(json!({
            "group_key": "acme",
            "group_properties": {"plan": "pro"},
            "created_at": "2025-01-02T03:04:05Z"
        }))
        .unwrap();

        let event = group.to_pipeline_event(
            &config,
            &GroupTypeMap::new(config.posthog_group_types.clone()),
            &group_type,
        );
        assert_eq!(event.event, "$groupidentify");
        assert_eq!(event.group0.as_deref(), Some("acme"));
        assert_eq!(
            event.group_properties,
            Some(json!({"company": {"plan": "pro"}}))
        );
    }

    #[test]
    fn group_import_event_uses_stable_uuid() {
        let config = base_config();
        let group_type = PostHogGroupType {
            name: "company".to_string(),
            index: 0,
        };
        let group: PostHogGroup = serde_json::from_value(json!({
            "group_key": "acme"
        }))
        .unwrap();

        let first = group.to_pipeline_event(
            &config,
            &GroupTypeMap::new(config.posthog_group_types.clone()),
            &group_type,
        );
        let second = group.to_pipeline_event(
            &config,
            &GroupTypeMap::new(config.posthog_group_types.clone()),
            &group_type,
        );

        assert_eq!(first.uuid, second.uuid);
    }

    #[test]
    fn import_state_records_logical_keys() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.jsonl").to_string_lossy().to_string();
        let keys = vec![
            ImportKey::event("event-uuid".to_string()),
            ImportKey::person("person-event-uuid".to_string(), "user-1".to_string()),
            ImportKey::person_snapshot("person-snapshot-uuid".to_string(), "person-1".to_string()),
            ImportKey::group(
                "group-event-uuid".to_string(),
                "company".to_string(),
                "acme".to_string(),
            ),
        ];

        let mut state = ImportState::load(Some(path.clone())).unwrap();
        state.record(&keys).unwrap();
        let state = ImportState::load(Some(path)).unwrap();

        for key in keys {
            assert!(state.contains(&key));
        }
    }

    #[test]
    fn event_row_hydrates_person_and_group_snapshots() {
        let config = base_config();
        let posthog = PostHogClient::new(
            config.posthog_host.clone(),
            config.posthog_project_id.clone(),
            config.posthog_environment_id.clone(),
            config.posthog_personal_api_key.clone(),
            config.pipeline_timeout,
        )
        .unwrap();
        let pipeline = PipelineClient::new(
            config.pipeline_endpoint.clone(),
            None,
            Duration::from_secs(1),
        )
        .unwrap();
        let mut importer = Importer::new(config, posthog, pipeline, None).unwrap();
        importer.persons_by_distinct_id.insert(
            "user-1".to_string(),
            ImportedPersonSnapshot {
                person_id: Some("person-uuid".to_string()),
                created_at: parse_datetime("2025-01-01T00:00:00Z"),
                properties: Some(json!({"email": "u@example.com"})),
            },
        );
        importer.group_properties.insert(
            ("company".to_string(), "acme".to_string()),
            serde_json::from_value(json!({"plan": "pro"})).unwrap(),
        );

        let event = importer
            .row_to_pipeline_event(
                EventRow::from_value(json!([
                    "event-uuid",
                    "purchase",
                    "user-1",
                    "2025-01-03T00:00:00Z",
                    "2025-01-03T00:00:01Z",
                    {"amount": 99, "$groups": {"company": "acme"}}
                ]))
                .unwrap(),
            )
            .unwrap()
            .unwrap();

        assert_eq!(event.uuid, "event-uuid");
        assert_eq!(event.event, "purchase");
        assert_eq!(event.person_id.as_deref(), Some("person-uuid"));
        assert_eq!(event.group0.as_deref(), Some("acme"));
        assert_eq!(
            event.group_properties,
            Some(json!({"company": {"plan": "pro"}}))
        );
        assert_eq!(event.properties.unwrap()["amount"], json!(99));
    }

    #[test]
    fn parses_page_envelopes_arrays_and_singletons() {
        assert_eq!(
            page_items(&json!({"results": [{"id": 1}], "next": null})),
            vec![json!({"id": 1})]
        );
        assert_eq!(page_items(&json!([{"id": 1}])), vec![json!({"id": 1})]);
        assert_eq!(page_items(&json!({"id": 1})), vec![json!({"id": 1})]);
    }
}
