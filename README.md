# Hogflare

<img src="hog.png" alt="Hogflare" width="300">

Hogflare is a Cloudflare Workers service that accepts PostHog ingestion requests and forwards them into Cloudflare Pipelines (HTTP stream). It gives you PostHog-compatible ingestion with data landing in R2 as Iceberg/Parquet.

## Why?

PostHog is a nice-to-use web & product analytics platform. However, self-hosting PostHog is prohibitively complex so most users seem to rely on the cloud offering. This is an alternative for cost-conscious data folks & businesses interested in a low maintenance way to ingest web & product analytics directly into a managed data lake. 

A [hobby deployment of PostHog](https://github.com/PostHog/posthog/blob/master/docker-compose.hobby.yml) includes: postgres, redis, redis7, clickhouse, zookeeper, kafka, worker, web, plugins, proxy, objectstorage, seaweedfs, asyncmigrationscheck, temporal, elasticsearch, temporal-admin-tools, temporal-ui, temporal-django-worker, cyclotron-janitor, capture, replay-capture, property-defs-rs, livestream, feature-flags, cymbal

Admittedly, PostHog does a *lot* more than this package, but some folks really just want the basics!

## Quick start (Cloudflare)

1) Create a Pipeline stream and sink in the Cloudflare dashboard or via `wrangler pipelines setup`.
2) Use the schema below for the stream.
3) Create a local `wrangler.toml` (gitignored) and set variables.
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

### Local `wrangler.toml` (gitignored)

```toml
name = "hogflare"
main = "build/index.js"
compatibility_date = "2025-01-09"

[vars]
CLOUDFLARE_PIPELINE_ENDPOINT = "https://<stream-id>.ingest.cloudflare.com"
CLOUDFLARE_PIPELINE_TIMEOUT_SECS = "10"
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

## Query data (DuckDB)

```sql
INSTALL httpfs;
INSTALL iceberg;
LOAD httpfs;
LOAD iceberg;

CREATE SECRET r2_secret (
  TYPE S3,
  KEY_ID '<R2_ACCESS_KEY_ID>',
  SECRET '<R2_SECRET_ACCESS_KEY>',
  ENDPOINT '<ACCOUNT_ID>.r2.cloudflarestorage.com',
  REGION 'auto'
);

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

## Event shape in R2

Each row is a `PipelineEvent` with these columns:

- `source`
- `event_type`
- `distinct_id`
- `timestamp` (RFC3339)
- `properties` (JSON)
- `context` (JSON)
- `person_properties` (JSON)
- `api_key`
- `extra` (JSON)

## Enrichment

Hogflare adds Cloudflare request data into `properties` when those keys are not already present:

- `$ip` from `CF-Connecting-IP`
- `$geoip_*` from Cloudflare request metadata (country, city, region, lat/long, timezone)
- `cf_*` fields: `cf_asn`, `cf_as_organization`, `cf_colo`, `cf_metro_code`, `cf_ray`

## Limitations

- This repo does not include a local Pipelines emulator. You need a real Cloudflare Pipeline endpoint.
- `/decide` returns placeholders, not evaluated flags.
- `/s` stores raw session recording chunks only.
