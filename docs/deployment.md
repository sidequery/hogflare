# Deployment

## Quick Start

1. Create R2 Data Catalog-backed Pipelines resources.
2. Copy `wrangler.toml.example` to `wrangler.toml` and set the stream endpoints.
3. Set Wrangler secrets.
4. Build and deploy the Worker.
5. Send a capture/identify verification flow and query the Iceberg tables.

The examples below use stable table names for a fresh deployment: `default.hogflare_events` and `default.hogflare_persons`. If you use versioned names during migration, substitute those names consistently in the sink commands and queries.

## Create Pipeline Resources

Set these values before creating sinks:

```bash
export R2_BUCKET="<bucket-name>"
export R2_CATALOG_TOKEN="<r2-data-catalog-token>"
```

`R2_CATALOG_TOKEN` is the token used by R2 Data Catalog/R2 SQL clients such as DuckDB or PyIceberg. The bucket must have R2 Data Catalog enabled before creating `r2-data-catalog` sinks.

Create the events stream, sink, and pipeline:

```bash
bunx wrangler pipelines streams create hogflare_events_stream \
  --schema-file scripts/events-pipeline-schema.json \
  --http-enabled true \
  --http-auth true

bunx wrangler pipelines sinks create hogflare_events_sink \
  --type r2-data-catalog \
  --bucket "$R2_BUCKET" \
  --namespace default \
  --table hogflare_events \
  --catalog-token "$R2_CATALOG_TOKEN" \
  --roll-interval 60

bunx wrangler pipelines create hogflare_events_pipeline \
  --sql "INSERT INTO hogflare_events_sink SELECT * FROM hogflare_events_stream;"
```

Create the persons stream, sink, and pipeline if you want queryable people in Iceberg:

```bash
bunx wrangler pipelines streams create hogflare_persons_stream \
  --schema-file scripts/persons-pipeline-schema.json \
  --http-enabled true \
  --http-auth true

bunx wrangler pipelines sinks create hogflare_persons_sink \
  --type r2-data-catalog \
  --bucket "$R2_BUCKET" \
  --namespace default \
  --table hogflare_persons \
  --catalog-token "$R2_CATALOG_TOKEN" \
  --roll-interval 60

bunx wrangler pipelines create hogflare_persons_pipeline \
  --sql "INSERT INTO hogflare_persons_sink SELECT * FROM hogflare_persons_stream;"
```

Each stream creation command prints an HTTP endpoint like `https://<stream-id>.ingest.cloudflare.com`. Use those endpoints in `wrangler.toml`.

## Wrangler Config

Copy the example and fill in the stream endpoints:

```bash
cp wrangler.toml.example wrangler.toml
```

```toml
name = "hogflare"
main = "build/index.js" # generated entrypoint from worker-build for the Rust worker
compatibility_date = "2025-01-09"

[vars]
CLOUDFLARE_PIPELINE_ENDPOINT = "https://<stream-id>.ingest.cloudflare.com"
CLOUDFLARE_PERSONS_PIPELINE_ENDPOINT = "https://<persons-stream-id>.ingest.cloudflare.com"
CLOUDFLARE_PIPELINE_TIMEOUT_SECS = "10"

# Optional
# POSTHOG_TEAM_ID = "1"
# POSTHOG_GROUP_TYPE_0 = "company"
# POSTHOG_GROUP_TYPE_1 = "team"
# POSTHOG_GROUP_TYPE_2 = "project"
# POSTHOG_GROUP_TYPE_3 = "org"
# POSTHOG_GROUP_TYPE_4 = "workspace"
# POSTHOG_SESSION_RECORDING_ENDPOINT = "/s/"

[[durable_objects.bindings]]
name = "PERSONS"
class_name = "PersonDurableObject"

[[durable_objects.bindings]]
name = "PERSON_ID_COUNTER"
class_name = "PersonIdCounterDurableObject"

[[durable_objects.bindings]]
name = "GROUPS"
class_name = "GroupDurableObject"

[[migrations]]
tag = "v1"
new_sqlite_classes = ["PersonDurableObject"]

[[migrations]]
tag = "v2"
new_sqlite_classes = ["PersonIdCounterDurableObject", "GroupDurableObject"]
```

## Configuration Reference

| Setting | Required | Notes |
| --- | --- | --- |
| `CLOUDFLARE_PIPELINE_ENDPOINT` | Yes | Events stream HTTP endpoint from `wrangler pipelines streams create`. |
| `CLOUDFLARE_PIPELINE_AUTH_TOKEN` | Yes, for authenticated streams | Bearer token used for events stream HTTP ingest. |
| `CLOUDFLARE_PERSONS_PIPELINE_ENDPOINT` | No | Persons stream endpoint. Set this to write person snapshots to Iceberg. |
| `CLOUDFLARE_PERSONS_PIPELINE_AUTH_TOKEN` | No | Falls back to `CLOUDFLARE_PIPELINE_AUTH_TOKEN` when omitted. |
| `CLOUDFLARE_PIPELINE_TIMEOUT_SECS` | No | Defaults to 10 seconds. |
| `POSTHOG_API_KEY` | No | Default project token returned by `/decide` when request/header token is absent. |
| `POSTHOG_TEAM_ID` | No | Optional team id attached to event and person rows. |
| `POSTHOG_GROUP_TYPE_0..4` | No | Maps PostHog group types to `group0..group4`; set `POSTHOG_GROUP_TYPE_0=company` to populate `group0` for company groups. |
| `POSTHOG_SESSION_RECORDING_ENDPOINT` | No | Returned in `/decide` session recording config. |
| `HOGFLARE_REPLAY_ACCOUNT_ID` | No | Enables the `/replay` UI API when set with bucket and token. |
| `HOGFLARE_REPLAY_BUCKET` | No | R2 bucket name backing the R2 Data Catalog warehouse. |
| `HOGFLARE_REPLAY_R2_SQL_TOKEN` | No | R2 SQL/Data Catalog token used server-side to query replay rows. Store as a secret. |
| `HOGFLARE_REPLAY_EVENTS_TABLE` | No | Iceberg events table queried by replay APIs. Defaults to `default.hogflare_events`. |
| `HOGFLARE_REPLAY_QUERY_LIMIT` | No | Maximum snapshot rows a replay API request can read. Defaults to `5000`. |
| `POSTHOG_SIGNING_SECRET` | No | Enables HMAC request signature checks. |
| `PERSON_DEBUG_TOKEN` | No | Enables `/__debug/person/:id` for deployment verification. |
| `HOGFLARE_FEATURE_FLAGS` | No | JSON flag config used by `/decide` and `/flags`. |

## Secrets

Use a Cloudflare API token that can write to Pipelines for `CLOUDFLARE_PIPELINE_AUTH_TOKEN`. The same token can usually be reused for the persons stream.

```bash
bunx wrangler secret put CLOUDFLARE_PIPELINE_AUTH_TOKEN
# Optional. If omitted, the persons pipeline uses CLOUDFLARE_PIPELINE_AUTH_TOKEN.
bunx wrangler secret put CLOUDFLARE_PERSONS_PIPELINE_AUTH_TOKEN

# Optional.
bunx wrangler secret put POSTHOG_SIGNING_SECRET
bunx wrangler secret put PERSON_DEBUG_TOKEN
bunx wrangler secret put HOGFLARE_FEATURE_FLAGS
bunx wrangler secret put HOGFLARE_REPLAY_R2_SQL_TOKEN
```

## Deploy

```bash
worker-build --release
bunx wrangler deploy
```

## Verify Deployment

```bash
export HOGFLARE_URL="https://<your-worker>.workers.dev"
export HOGFLARE_API_KEY="phc_verify_$(date -u +%Y%m%d%H%M%S)"
export HOGFLARE_ANON_ID="${HOGFLARE_API_KEY}_anon"
export HOGFLARE_USER_ID="${HOGFLARE_API_KEY}_user"
```

Send an anonymous capture:

```bash
curl -X POST "$HOGFLARE_URL/capture" \
  -H "Content-Type: application/json" \
  -d "{
    \"api_key\": \"$HOGFLARE_API_KEY\",
    \"event\": \"verify-anon-capture\",
    \"distinct_id\": \"$HOGFLARE_ANON_ID\",
    \"properties\": {
      \"\$set\": { \"initial_referrer\": \"docs\" },
      \"\$set_once\": { \"first_seen_source\": \"readme\" }
    }
  }"
```

Identify the user and link the anonymous ID:

```bash
curl -X POST "$HOGFLARE_URL/identify" \
  -H "Content-Type: application/json" \
  -d "{
    \"api_key\": \"$HOGFLARE_API_KEY\",
    \"distinct_id\": \"$HOGFLARE_USER_ID\",
    \"properties\": {
      \"\$anon_distinct_id\": \"$HOGFLARE_ANON_ID\",
      \"\$set\": { \"email\": \"verify@example.com\", \"plan\": \"pro\" },
      \"\$set_once\": { \"signup_source\": \"readme\" }
    }
  }"
```

Send a post-identify capture:

```bash
curl -X POST "$HOGFLARE_URL/capture" \
  -H "Content-Type: application/json" \
  -d "{
    \"api_key\": \"$HOGFLARE_API_KEY\",
    \"event\": \"verify-identified-capture\",
    \"distinct_id\": \"$HOGFLARE_USER_ID\",
    \"properties\": { \"button\": \"verify\" }
  }"
```

Wait for the sink roll interval, then query R2 SQL:

```bash
export R2_WAREHOUSE="<account-id>_<bucket-name>"
export WRANGLER_R2_SQL_AUTH_TOKEN="$R2_CATALOG_TOKEN"

bunx wrangler r2 sql query "$R2_WAREHOUSE" \
  "select event, distinct_id, person_id, person_properties
   from default.hogflare_events
   where api_key = '$HOGFLARE_API_KEY'
   order by created_at asc"

bunx wrangler r2 sql query "$R2_WAREHOUSE" \
  "select operation, canonical_distinct_id, person_id, distinct_ids, merged_properties
   from default.hogflare_persons
   where api_key = '$HOGFLARE_API_KEY'
   order by updated_at asc"
```

Expected result: the three event rows share one `person_id`, and the persons table has `capture`, `identify`, `capture` snapshots. After identify, `distinct_ids` should include both the anonymous and identified IDs.

## Local Development

The repo includes a lightweight fake pipeline used by tests.

```bash
docker compose up --build -d fake-pipeline
```

```bash
# .env.local (not committed)
CLOUDFLARE_PIPELINE_ENDPOINT=http://127.0.0.1:8088/
CLOUDFLARE_PERSONS_PIPELINE_ENDPOINT=http://127.0.0.1:8088/
CLOUDFLARE_PIPELINE_TIMEOUT_SECS=5
```

```bash
cargo run --bin hogflare
```

## Cleanup

Delete Pipelines resources in dependency order: pipelines first, then streams and sinks.

```bash
bunx wrangler pipelines list
bunx wrangler pipelines delete <pipeline-id> --force

bunx wrangler pipelines streams list
bunx wrangler pipelines streams delete <stream-id> --force

bunx wrangler pipelines sinks list
bunx wrangler pipelines sinks delete <sink-id> --force
```

`wrangler r2 sql query` is read-only. To drop an Iceberg table from R2 Data Catalog, use the Iceberg catalog API. One local option is PyIceberg:

```bash
R2_CATALOG_TOKEN="<r2-data-catalog-token>" uv run --with pyiceberg python - <<'PY'
import os
from pyiceberg.catalog.rest import RestCatalog

catalog = RestCatalog(
    name="hogflare",
    warehouse="<account-id>_<bucket-name>",
    uri="https://catalog.cloudflarestorage.com/<account-id>/<bucket-name>",
    token=os.environ["R2_CATALOG_TOKEN"],
)

catalog.drop_table(("default", "<table-name>"), purge_requested=True)
PY
```
