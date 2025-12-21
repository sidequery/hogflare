# Hogflare

<img src="hog.png" alt="Hogflare" width="300">

An Axum-based PostHog compatible ingestion layer that forwards events to a Cloudflare Pipeline HTTP stream. The service supports the `/capture` and `/identify` endpoints so existing PostHog SDKs can drop-in with minimal configuration changes.

## Why?

Posthog is a nice to use web & product analytics platform. However, self hosting Posthog is prohibitively complex so most users seem to rely on the cloud offering. This is an alternative for cost-conscious data folks & businesses interested in a low maintainence way to ingest web & product analytics directly into a managed data lake. 

A [hobby deployment of Posthog](https://github.com/PostHog/posthog/blob/master/docker-compose.hobby.yml) includes: postgres, redis, redis7, clickhouse, zookeeper, kafka, worker, web, plugins, proxy, objectstorage, seaweedfs, asyncmigrationscheck, temporal, elasticsearch, temporal-admin-tools, temporal-ui, temporal-django-worker, cyclotron-janitor, capture, replay-capture, property-defs-rs, livestream, feature-flags, cymbal

Admittedley, Posthog does a *lot* more than this packages, but some folks really just want the basics!

## Getting started

### Prerequisites

* Rust toolchain (`cargo`, `rustc`)
* Docker (for Cloudflare Container deployment)
* Node.js and npm/bun (for Cloudflare Workers deployment)
* A Cloudflare account with Pipelines, R2, and Containers enabled

### Configuration

The server is configured through environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `APP_ADDR` | `0.0.0.0:8080` | Address the Axum server binds to. |
| `CLOUDFLARE_PIPELINE_ENDPOINT` | _required_ | The HTTPS ingestion endpoint created by `wrangler pipelines setup`. |
| `CLOUDFLARE_PIPELINE_AUTH_TOKEN` | _optional_ | Bearer token to attach to pipeline requests when authentication is enabled. |
| `CLOUDFLARE_PIPELINE_TIMEOUT_SECS` | `10` | Timeout (in seconds) applied to the pipeline HTTP client. |
| `POSTHOG_API_KEY` | _optional_ | Default project API key returned from `/decide` when the client does not supply one. |
| `POSTHOG_SESSION_RECORDING_ENDPOINT` | _optional_ | Session recording proxy target surfaced in `/decide` responses and used by the `/s` ingestion stub. |
| `POSTHOG_SIGNING_SECRET` | _optional_ | Shared secret used to validate `X-POSTHOG-SIGNATURE`/`X-Hub-Signature` headers. When set, signatures are required. |

Create a `.env` file with the required values:

```env
APP_ADDR=0.0.0.0:8080
CLOUDFLARE_PIPELINE_ENDPOINT=https://{stream-id}.ingest.cloudflare.com
CLOUDFLARE_PIPELINE_AUTH_TOKEN=your_token_if_required
```

Load the environment and start the server:

```bash
cargo run
```

### Local pipeline emulator

For end-to-end testing without Cloudflare you can launch the included FastAPI + DuckDB pipeline stub:

```bash
docker compose up -d fake-pipeline
```

The emulator listens on `http://127.0.0.1:8088/`. Hogflare tests use this service to verify that PostHog JS events persist into DuckDB; stop it when you are done:

```bash
docker compose down --remove-orphans
```

### Endpoint coverage

- [x] Event capture `/capture` (JSON or form payloads, header/body API keys, gzip/deflate/gzip-js compression)
- [x] Identify `/identify` (forwards `$identify`, applies shared API keys)
- [x] Group updates `/groups` (handles `$groupidentify` with type and key metadata)
- [x] Batch ingest `/batch` (mixed capture/identify/group/alias/engage payloads with compression fallback)
- [x] Alias merge `/alias` (forwards `$create_alias`, backfills alias metadata into `extra`)
- [x] People updates `/engage` (supports `$set`, `$set_once`, `$unset`, `$group_set`)
- [x] Decide `/decide` (returns placeholder flag payload, surfaces project API key and session recording endpoint)
- [x] Session recording `/s` (validates HMAC when configured, forwards `$snapshot` chunks)
- [x] Health probe `/healthz` (simple liveness response)

All handlers return PostHog-compatible responses (`{"status": 1}` success, `{"status": 0}` failure).

Hogflare aims to be drop-in compatible with common PostHog SDK calls, but a few behaviours differ from the official PostHog server:

* **Session recording storage**: `/s` forwards snapshots to the pipeline, but replay requires a downstream sink that understands the payload format.
* **Feature flag resolution**: `/decide` returns static placeholders rather than running flag evaluation.
* **Plugin / ingestion pipeline hooks**: PostHog plugins and data transformation hooks are not executed.
* **Legacy endpoints**: older `/api/event/` and SDK-specific routes proxy through `/capture`; add dedicated handlers if you rely on them.

### Deploying to Cloudflare

1. Set up Cloudflare infrastructure (R2 bucket and Pipeline):
   ```bash
   ./scripts/setup-cloudflare.sh
   ```

2. Run the interactive pipeline setup:
   ```bash
   bunx wrangler pipelines setup
   ```
   Use the generated `scripts/pipeline-schema.json` file and note the endpoint URL and auth token.

3. Create `.env.local` with your credentials:
   ```bash
   cp .env.local .env.local
   # Edit .env.local and fill in:
   # - CLOUDFLARE_PIPELINE_ENDPOINT
   # - CLOUDFLARE_PIPELINE_AUTH_TOKEN
   ```

4. Install dependencies and deploy:
   ```bash
   bun install
   bunx wrangler deploy
   ```

The service will be deployed as a Cloudflare Container managed by a Worker. Environment variables are loaded from `.env.local`.

### Sending sample data

You can verify end-to-end delivery with curl:

```bash
curl -X POST http://localhost:8080/capture \
  -H "Content-Type: application/json" \
  -d '{
        "api_key": "phc_example",
        "event": "purchase",
        "distinct_id": "user_12345",
        "properties": { "amount": 29.99, "product_id": "widget-001" }
      }'
```

The service forwards the payload to Cloudflare and returns `{"status":1}` if the pipeline accepts the event.

### Querying data

Once the pipeline writes data into R2 you can query it with R2 SQL:

```bash
export WRANGLER_R2_SQL_AUTH_TOKEN=YOUR_API_TOKEN
npx wrangler r2 sql query "YOUR_WAREHOUSE_NAME" "
SELECT distinct_id, event_type, properties
FROM default.ecommerce
ORDER BY timestamp DESC
LIMIT 10
"
```

Refer to the [Cloudflare Pipelines documentation](https://developers.cloudflare.com/pipelines/) for more advanced configurations, retention policies, and integrating additional sinks.
