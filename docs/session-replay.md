# Session Replay

![Hogflare replay explorer](assets/replay-explorer.jpg)

Hogflare stores replay uploads in the same events table as analytics events, then serves a read-only replay explorer from `/replay`. The UI is for product analytics workflows: browse recent sessions, search events, inspect funnel drop-offs, review computed friction signals, and follow a person journey.

## Ingestion

- SDK config advertises `sessionRecording: false` when `POSTHOG_SESSION_RECORDING_ENDPOINT` is unset.
- Set `POSTHOG_SESSION_RECORDING_ENDPOINT=/s/` to route replay uploads through Hogflare.
- `/s` accepts PostHog replay payloads, including gzip/gzip-js compressed browser SDK requests.
- Modern `$snapshot` payloads are normalized to `$snapshot_items` rows before they are sent through Cloudflare Pipelines into R2.
- Legacy raw chunk payloads are still accepted as `$snapshot` rows.

## Replay API Config

Replay APIs require:

| Setting | Notes |
| --- | --- |
| `HOGFLARE_REPLAY_ACCOUNT_ID` | Cloudflare account id for the R2 SQL endpoint. |
| `HOGFLARE_REPLAY_BUCKET` | R2 bucket name backing the R2 Data Catalog warehouse. |
| `HOGFLARE_REPLAY_R2_SQL_TOKEN` | R2 SQL/Data Catalog token. Store as a secret. |
| `HOGFLARE_REPLAY_EVENTS_TABLE` | Optional Iceberg table. Defaults to `default.hogflare_events`. |
| `HOGFLARE_REPLAY_QUERY_LIMIT` | Optional maximum rows read per replay API request. Defaults to `5000`. |
| `HOGFLARE_REPLAY_R2_SQL_ENDPOINT` | Optional override used by tests and local demos. |

The token stays server-side in the Worker. The browser only calls Hogflare's replay API.

## Routes

- `/replay` serves the explorer UI.
- `/replay/api/sessions` lists replay sessions by reading `$snapshot_items` and legacy `$snapshot` rows from Iceberg through R2 SQL.
- `/replay/api/events` searches analytics events while excluding replay recording rows.
- `/replay/api/funnels` classifies sessions as converted, stuck, or dropped for an ordered `steps` list of event names.
- `/replay/api/friction` computes replay-derived signals such as rage clicks, dead clicks, form thrash, long idle gaps, repeated navigation, and deep scroll without follow-up.
- `/replay/api/person` joins a distinct ID's replay sessions and analytics events into one journey timeline.
- `/replay/api/sessions/:session_id` returns normalized rrweb events plus an activity timeline for one session.

## Filters

These query parameters can narrow replay reads:

- `api_key`
- `distinct_id`
- `session_id`
- `url`
- `event_name`
- `steps`
- `signal`
- `date_from`
- `date_to`
- `min_duration_secs`
- `max_duration_secs`
- `min_events`
- `max_events`
- `limit`

Session deep links use `session_id` plus `at_ms`.

## Local Demo

Start the replay SQL demo stub:

```bash
REPLAY_DEMO_PORT=4666 bun scripts/replay_demo_stub.mjs
```

Start Hogflare against the stub:

```bash
APP_ADDR=127.0.0.1:4567 \
CLOUDFLARE_PIPELINE_ENDPOINT=http://127.0.0.1:4666/ \
HOGFLARE_REPLAY_ACCOUNT_ID=demo-account \
HOGFLARE_REPLAY_BUCKET=demo-bucket \
HOGFLARE_REPLAY_R2_SQL_TOKEN=demo-token \
HOGFLARE_REPLAY_R2_SQL_ENDPOINT=http://127.0.0.1:4666/ \
HOGFLARE_REPLAY_EVENTS_TABLE=default.hogflare_events \
HOGFLARE_REPLAY_QUERY_LIMIT=500 \
cargo run --bin hogflare
```

Open:

```text
http://127.0.0.1:4567/replay?api_key=phc_demo&distinct_id=replay-user&limit=100&session_id=demo-session-1&at_ms=1500
```
