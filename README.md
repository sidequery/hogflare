# Hogflare

<img src="hog.png" alt="Hogflare" width="300">

Hogflare is a Cloudflare Workers service that accepts PostHog ingestion requests and forwards them into Cloudflare Pipelines (HTTP stream). It gives you PostHog-compatible ingestion with data landing in R2 as Iceberg/Parquet.

## Architecture

```
PostHog SDKs
   |
   v
Cloudflare Worker (Hogflare)
   | \
   |  \__ Durable Object (persons)
   |       - merges $set / $set_once
   |       - applies aliasing
   |
   v
Cloudflare Pipelines (HTTP stream)
   |
   v
R2 (Iceberg / Parquet)
```

Notes:
- Events are always forwarded to the pipeline.
- Person updates are applied statefully in a Durable Object keyed by distinct_id.

## Why?

PostHog is a nice-to-use web & product analytics platform. However, self-hosting PostHog is prohibitively complex so most users seem to rely on the cloud offering. This is an alternative for cost-conscious data folks & businesses interested in a low maintenance way to ingest web & product analytics directly into a managed data lake.

A [hobby deployment of PostHog](https://github.com/PostHog/posthog/blob/master/docker-compose.hobby.yml) includes: postgres, redis, redis7, clickhouse, zookeeper, kafka, worker, web, plugins, proxy, objectstorage, seaweedfs, asyncmigrationscheck, temporal, elasticsearch, temporal-admin-tools, temporal-ui, temporal-django-worker, cyclotron-janitor, capture, replay-capture, property-defs-rs, livestream, feature-flags, cymbal

Admittedly, PostHog does a *lot* more than this package, but some folks really just want the basics!

## Quick start (Cloudflare)

1) Create a Pipeline stream and sink in the Cloudflare dashboard or via `wrangler pipelines setup`.
2) Use the schema below for the stream.
3) Copy `wrangler.toml.example` to `wrangler.toml` and set variables.
4) Set Wrangler secrets.
5) Deploy the Worker.

### Pipeline schema (JSON)

```json
{
  "fields": [
    { "name": "source", "type": "string", "required": true },
    { "name": "event_type", "type": "string", "required": true },
    { "name": "distinct_id", "type": "string", "required": true },
    { "name": "timestamp", "type": "timestamp", "required": false },
    { "name": "properties", "type": "json", "required": false },
    { "name": "context", "type": "json", "required": false },
    { "name": "person_properties", "type": "json", "required": false },
    { "name": "api_key", "type": "string", "required": false },
    { "name": "extra", "type": "json", "required": false }
  ]
}
```

### Wrangler config

Copy the example and fill in your stream endpoint:

```bash
cp wrangler.toml.example wrangler.toml
```

```toml
name = "hogflare"
main = "build/index.js" # generated entrypoint from worker-build for the Rust worker
compatibility_date = "2025-01-09"

[vars]
CLOUDFLARE_PIPELINE_ENDPOINT = "https://<stream-id>.ingest.cloudflare.com"
CLOUDFLARE_PIPELINE_TIMEOUT_SECS = "10"

[[durable_objects.bindings]]
name = "PERSONS"
class_name = "PersonDurableObject"

[[migrations]]
tag = "v1"
new_classes = ["PersonDurableObject"]
```

### Secrets

```bash
bunx wrangler secret put CLOUDFLARE_PIPELINE_AUTH_TOKEN
bunx wrangler secret put POSTHOG_SIGNING_SECRET
```

### Deploy

```bash
bunx wrangler deploy
```

## Send a test event

```bash
curl -X POST https://<your-worker>.workers.dev/capture \
  -H "Content-Type: application/json" \
  -d '[
    {
      "api_key": "phc_example",
      "event": "purchase",
      "distinct_id": "user_12345",
      "properties": { "amount": 29.99, "product_id": "widget-001" }
    }
  ]'
```

## HMAC signing (optional)

If `POSTHOG_SIGNING_SECRET` is set, requests must include a valid signature.

```bash
payload='[
  {
    "api_key": "phc_example",
    "event": "purchase",
    "distinct_id": "user_12345",
    "properties": { "amount": 29.99 }
  }
]'

signature=$(printf '%s' "$payload" | openssl dgst -sha256 -hmac "$POSTHOG_SIGNING_SECRET" | awk '{print $2}')

curl -X POST https://<your-worker>.workers.dev/capture \
  -H "Content-Type: application/json" \
  -H "X-POSTHOG-SIGNATURE: sha256=$signature" \
  -d "$payload"
```

Note: `X-HUB-SIGNATURE` with `sha1=` is also accepted for GitHub-style webhook compatibility.

## PostHog SDK config

### Browser (posthog-js)

```js
import posthog from "posthog-js";

posthog.init("<project_api_key>", {
  api_host: "https://<your-worker>.workers.dev",
  capture_pageview: true,
});
```

### Server (posthog-node)

```js
import { PostHog } from "posthog-node";

const client = new PostHog("<project_api_key>", {
  host: "https://<your-worker>.workers.dev",
});

client.capture({
  distinctId: "user_123",
  event: "purchase",
  properties: { amount: 29.99 },
});

await client.shutdown();
```

### Other SDKs

Set the SDK host/base URL to your Worker (`https://<your-worker>.workers.dev`) and use your project API key. Most SDKs use either `api_host` (browser/mobile) or `host` (server).

## Local development (fake pipeline)

The repo includes a lightweight fake pipeline (FastAPI + DuckDB) used by tests.

```bash
docker compose up --build -d fake-pipeline
```

```bash
# .env.local (not committed)
CLOUDFLARE_PIPELINE_ENDPOINT=http://127.0.0.1:8088/
CLOUDFLARE_PIPELINE_TIMEOUT_SECS=5
```

```bash
cargo run
```

## Query data (DuckDB)

```sql
INSTALL httpfs;
INSTALL iceberg;
LOAD httpfs;
LOAD iceberg;

CREATE SECRET r2_catalog_secret (
  TYPE ICEBERG,
  TOKEN '<CLOUDFLARE_API_TOKEN>'
);

ATTACH '<ACCOUNT_ID>_<BUCKET>' AS iceberg_catalog (
  TYPE ICEBERG,
  ENDPOINT 'https://catalog.cloudflarestorage.com/<ACCOUNT_ID>/<BUCKET>'
);

SELECT count(*) FROM iceberg_catalog.default.hogflare;
SELECT * FROM iceberg_catalog.default.hogflare LIMIT 5;
```

## PostHog compatibility

### Ingestion endpoints

- `/capture` (single or batch payloads)
- `/identify`
- `/alias`
- `/batch` (mixed events)
- `/e` (event payloads)

### Persons

Identify, capture `$set` / `$set_once`, and alias events update a person record stored in a Durable Object. The record tracks distinct_id aliases plus merged person properties. This state is separate from the pipeline data; it is not written into R2.

### Groups

- `/groups` (`$groupidentify` payloads) are forwarded.
- Group state is not evaluated server-side.

### Session replay

- `/i/v0/e` stores raw session recording chunks only.
- `/s` stores raw session recording chunks only.

### Feature flags

- `/decide` returns placeholders, not evaluated flags.

### Signing

- If `POSTHOG_SIGNING_SECRET` is set, requests must include a valid HMAC signature.

### Enrichment

Hogflare adds Cloudflare request data into `properties` when those keys are not already present:

- `$ip` from `CF-Connecting-IP`
- `$geoip_*` from Cloudflare request metadata (country, city, region, lat/long, timezone)
- `cf_*` fields: `cf_asn`, `cf_as_organization`, `cf_colo`, `cf_metro_code`, `cf_ray`

## Event shape in R2

Each row is a `PipelineEvent` with these columns:

| Field | Type / Notes |
| --- | --- |
| `source` | string |
| `event_type` | string |
| `distinct_id` | string |
| `timestamp` | RFC3339 timestamp |
| `properties` | JSON |
| `context` | JSON |
| `person_properties` | JSON |
| `api_key` | string |
| `extra` | JSON |
