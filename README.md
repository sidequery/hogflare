# Hogflare

An Axum-based PostHog compatible ingestion layer that forwards events to a Cloudflare Pipeline HTTP stream. The service supports the `/capture` and `/identify` endpoints so existing PostHog SDKs can drop-in with minimal configuration changes.

## Getting started

### Prerequisites

* Rust toolchain (`cargo`, `rustc`)
* A Cloudflare account with Pipelines and R2 enabled
* An existing HTTP pipeline endpoint created via the Cloudflare Pipelines quickstart (see below)

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

### Supported endpoints

* `POST /capture` – accepts capture payloads; honors `X-POSTHOG-API-KEY` and `X-POSTHOG-SENT-AT` headers.
* `POST /identify` – accepts identify payloads and forwards them as `$identify` events.
* `POST /groups` – handles PostHog group identify payloads and forwards them as `$groupidentify`.
* `POST /batch` – accepts mixed capture/identify/group/alias/engage entries, applying shared API tokens when provided.
* `POST /alias` – forwards `$create_alias` events.
* `POST /engage` – ingests legacy `/engage` people updates (`$set`, `$set_once`, `$unset`, `$group_set`).
* `POST /decide` – returns a PostHog-compatible feature flag shell including configured API token and session recording proxy.
* `POST /s` and `POST /s/` – session recording ingestion stubs that always acknowledge uploads (useful for SDK compatibility).
* `GET /healthz` – liveness probe.

Responses mirror the PostHog ingestion API (`{"status": 1}` on success, `{"status": 0}` on error).

### Wiring Cloudflare Pipelines

Follow Cloudflare's tutorial to provision the required resources:

1. Create an R2 bucket and enable the data catalog:
   ```bash
   npx wrangler login
   npx wrangler r2 bucket create pipelines-tutorial
   npx wrangler r2 bucket catalog enable pipelines-tutorial
   ```
2. Create an API token with R2 catalog permissions via the Cloudflare dashboard.
3. Define the schema for your ingestion stream (for example, the ecommerce schema used in the tutorial) and run the interactive setup:
   ```bash
   cat > schema.json <<'JSON'
   {
     "fields": [
       { "name": "user_id", "type": "string", "required": true },
       { "name": "event_type", "type": "string", "required": true },
       { "name": "product_id", "type": "string", "required": false },
       { "name": "amount", "type": "float64", "required": false }
     ]
   }
   JSON

   npx wrangler pipelines setup
   ```
4. When prompted during setup, enable the HTTP endpoint and choose the Data Catalog table sink. Copy the generated endpoint URL and (if required) the authentication token – these values populate `CLOUDFLARE_PIPELINE_ENDPOINT` and `CLOUDFLARE_PIPELINE_AUTH_TOKEN` for the Axum service.
5. Point your PostHog SDKs at the Axum service. Captured events and identifies are transformed into a Cloudflare-friendly payload and relayed to the pipeline. Data will appear in the configured R2 bucket as Apache Iceberg files.

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

## Compatibility notes

Hogflare aims to be drop-in compatible with common PostHog SDK calls, but a few behaviours are still pending:

* **Session recording storage** – `/s` forwards snapshots to the pipeline, but replay still requires a downstream sink that understands the payload format.
* **Feature flag resolution** – `/decide` returns static placeholders rather than running flag evaluation.
* **Plugin / ingestion pipeline hooks** – PostHog plugins and data transformation hooks are not executed.
* **Legacy endpoints** – older `/api/event/` and SDK-specific routes still proxy through `/capture`; add dedicated handlers if you rely on them.

Pull requests covering these gaps are welcome.
