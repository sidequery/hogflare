# Import Existing PostHog Data

Hogflare includes a host-side importer for backfilling an existing PostHog project into the same Cloudflare Pipeline sink used by the Worker. It reads PostHog's private API with a personal API key, then writes normalized rows to the pipeline:

- persons as `$identify` event rows and, when configured, `PersonPipelineRecord` snapshots
- groups as `$groupidentify` rows
- historical events from HogQL with original `timestamp`, `created_at`, and PostHog event `uuid` when available

The importer writes historical rows directly to the pipeline. It does not mutate Worker Durable Object state.

## Required Inputs

```bash
export POSTHOG_PROJECT_ID="<project_id>"
export POSTHOG_PERSONAL_API_KEY="phx_..."
export CLOUDFLARE_PIPELINE_ENDPOINT="https://<stream-id>.ingest.cloudflare.com"
export CLOUDFLARE_PIPELINE_AUTH_TOKEN="<pipeline token>" # if your stream requires it
```

## Optional Inputs

```bash
export POSTHOG_HOST="https://us.posthog.com" # or https://eu.posthog.com / self-hosted URL
export POSTHOG_ENVIRONMENT_ID="<environment_id>" # recommended for current PostHog persons/groups APIs
export HOGFLARE_API_KEY="phc_..."
export POSTHOG_TEAM_ID="1"
export POSTHOG_GROUP_TYPE_0="company"
export IMPORT_FROM="2025-01-01"
export IMPORT_TO="2025-02-01"
export IMPORT_BATCH_SIZE="500"
export IMPORT_PERSONS_OFFSET="0" # resume guardrails
export IMPORT_EVENTS_OFFSET="0"
export IMPORT_EVENTS_AFTER_TIMESTAMP="2024-09-21T03:24:11Z"
export IMPORT_EVENTS_AFTER_UUID="0192129b-c354-77b4-b496-9be7ec571fb4"
export IMPORT_EVENT_UUIDS_FILE="/tmp/missing-event-uuids.txt"
export IMPORT_EVENT_WINDOW_DAYS="7"
export IMPORT_EVENT_WINDOW_HOURS="6" # use days or hours, not both
export IMPORT_MAX_PERSONS="1000" # optional guardrails for smoke tests
export IMPORT_MAX_GROUPS="1000"
export IMPORT_MAX_EVENTS="1000"
export IMPORT_STATE_FILE=".hogflare-import-state.jsonl"
export IMPORT_TARGET_ACCOUNT_ID="<cloudflare_account_id>"
export IMPORT_TARGET_BUCKET="<r2_bucket>"
export IMPORT_TARGET_TABLE="default.hogflare_events_v3"
export IMPORT_PERSONS_TARGET_TABLE="default.hogflare_persons_v2"
export WRANGLER_R2_SQL_AUTH_TOKEN="<r2 sql token>"
export CLOUDFLARE_PERSONS_PIPELINE_ENDPOINT="https://<persons-stream-id>.ingest.cloudflare.com"
export CLOUDFLARE_PERSONS_PIPELINE_AUTH_TOKEN="<persons pipeline token>" # falls back to event pipeline token
export IMPORT_CLOUDFLARE_API_TOKEN="<token with Pipelines read>" # optional auto flush discovery
export IMPORT_PIPELINE_FLUSH_SECS="300" # fallback if Pipelines read is unavailable
```

Production imports require R2 SQL target checks by default. The importer uses stable import keys, queries the target before each batch, and skips rows that are already present. Cloudflare Pipeline/R2 is append-only and does not enforce uniqueness by itself. Passing `--no-target-check` or `IMPORT_TARGET_CHECKS=false` opts out and should only be used for local tests.

Retry behavior is intentionally conservative. Import sends are not blindly retried after a transport or response error because the pipeline may have accepted the batch even if the client did not receive the response. The importer aligns its wait window to the Cloudflare Pipeline sink rolling policy when `IMPORT_CLOUDFLARE_API_TOKEN` can read Pipelines. Without that API access, it uses `IMPORT_PIPELINE_FLUSH_SECS`, defaulting conservatively to 300 seconds. The wait is `max(60s, 2 * flush + 30s)`, unless `IMPORT_TARGET_WAIT_SECS` is set explicitly.

The local state file makes normal same-machine resumes cheap, but it is not a substitute for target checks if the state file is lost, multiple importers run concurrently, or a send has an unknown commit state.

## Dry Run

```bash
cargo run --bin import_posthog -- --dry-run
```

## Run Import

```bash
cargo run --bin import_posthog
```

You can also pass flags instead of env vars:

```bash
cargo run --bin import_posthog -- \
  --posthog-host https://us.posthog.com \
  --project-id 12345 \
  --environment-id 67890 \
  --personal-api-key "$POSTHOG_PERSONAL_API_KEY" \
  --pipeline-endpoint "$CLOUDFLARE_PIPELINE_ENDPOINT" \
  --pipeline-auth-token "$CLOUDFLARE_PIPELINE_AUTH_TOKEN" \
  --persons-pipeline-endpoint "$CLOUDFLARE_PERSONS_PIPELINE_ENDPOINT" \
  --persons-pipeline-auth-token "$CLOUDFLARE_PERSONS_PIPELINE_AUTH_TOKEN" \
  --hogflare-api-key phc_example \
  --from 2025-01-01 \
  --to 2025-02-01 \
  --persons-offset 0 \
  --events-offset 0 \
  --events-after-timestamp 2024-09-21T03:24:11Z \
  --events-after-uuid 0192129b-c354-77b4-b496-9be7ec571fb4 \
  --event-uuids-file /tmp/missing-event-uuids.txt \
  --event-window-hours 6 \
  --max-persons 1000 \
  --max-groups 1000 \
  --max-events 1000 \
  --import-state-file .hogflare-import-state.jsonl \
  --target-account-id "$CLOUDFLARE_ACCOUNT_ID" \
  --target-bucket hogflare \
  --target-table default.hogflare_events_v3 \
  --persons-target-table default.hogflare_persons_v2 \
  --target-auth-token "$WRANGLER_R2_SQL_AUTH_TOKEN" \
  --cloudflare-api-token "$CLOUDFLARE_API_TOKEN"
```

Use `--skip-persons`, `--skip-groups`, or `--skip-events` to import only part of the project. Use `--skip-person-output` when resuming an event import after person rows were already written; it still loads people for event hydration.
