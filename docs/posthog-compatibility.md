# PostHog Compatibility

## SDK Config

### Browser

```js
import posthog from "posthog-js";

posthog.init("<project_api_key>", {
  api_host: "https://<your-worker>.workers.dev",
  capture_pageview: true,
});
```

### Server

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

Set the SDK host/base URL to your Worker (`https://<your-worker>.workers.dev`) and use your project API key. Most SDKs use either `api_host` for browser/mobile clients or `host` for server clients.

## Ingestion Endpoints

- `/capture` accepts single event payloads.
- `/identify` links and updates people.
- `/alias` creates aliases.
- `/batch` accepts mixed event payloads.
- `/e` accepts browser event payloads.
- `/i/v0/e` accepts PostHog error tracking event payloads.
- `/engage` accepts people updates.
- `/groups` accepts `$groupidentify` payloads.
- `/s` accepts session replay payloads.

## Error Tracking

PostHog SDK error tracking is supported through normal capture ingestion. `posthog.captureException(error, properties)` sends a `$exception` event with `$exception_list`, stack frames, mechanism metadata, optional `$exception_steps`, and any custom properties; Hogflare forwards those fields unchanged to the pipeline.

The documented PostHog manual ingestion endpoint `/i/v0/e/` is also available and uses the same browser event normalization as `/e/`. SDK remote config advertises exception autocapture support and serves the `exception-autocapture.js` helper expected by `posthog-js`.

Error tracking semantic models:

- `error_events` normalizes `$exception` rows into exception type, value, stack frame, fingerprint, user, session, URL, SDK, and grouping fields.
- `error_issues` groups exception events into issue rollups with first/last seen, event count, affected users, affected sessions, latest sample, and status.

Issue status is append-only. `PATCH /errors/api/issues/:fingerprint/status` writes a trusted `$error_issue_status` event with `status` set to `active`, `resolved`, or `ignored`; `error_issues` derives the latest trusted status for that project. The request must include an `api_key` identifying the project and an `x-hogflare-debug-token` header matching `PERSON_DEBUG_TOKEN`. Status updates are disabled when that token is not configured. Client-captured events named `$error_issue_status` are retained as ordinary events but cannot change issue state.

## Persons

Identify, capture `$set` / `$set_once` / `$unset`, and alias events update a person record stored in a Durable Object. The record tracks distinct ID aliases, person properties, and a sequential `id` plus a UUID.

Events include:

- `person_id`
- `person_created_at`
- `person_properties`

The Durable Object is the source of truth for the current person record. When `CLOUDFLARE_PERSONS_PIPELINE_ENDPOINT` is configured, Hogflare also writes append-only person snapshots to the persons pipeline so the state is queryable in Iceberg.

## Groups

- `/groups` (`$groupidentify` payloads) are forwarded.
- Group properties are stored in a Group DO and attached to events as `group_properties`.
- Group slots (`group0`..`group4`) are mapped by `POSTHOG_GROUP_TYPE_0..4`.

## Feature Flags

Feature flags and SDK remote config are evaluated in the Worker and exposed via `/array/:token/config`, `/decide`, and `/flags`.

Configuration is a JSON blob in `HOGFLARE_FEATURE_FLAGS`. It can be either:

- `{ "flags": [ ... ] }`
- `[ ... ]`

Supported fields per flag:

| Field | Type | Notes |
| --- | --- | --- |
| `key` | string | Flag key |
| `active` | bool | Defaults to `true` |
| `type` | `"boolean"` or `"multivariate"` | Defaults to boolean |
| `rollout_percentage` | number | 0 to 100 |
| `variants` | array | `[{ key, rollout_percentage, payload? }]` |
| `payload` | JSON | Used for boolean flags |
| `variant_payloads` | map | `{ "variant_key": { ... } }` |
| `conditions` | array | See filters below |
| `group_type` | string | Enables group-based rollout |
| `evaluation_environments` | array | Optional environment gating |
| `salt` | string | Optional bucketing salt |
| `id`, `version`, `description` | metadata | Returned in flag details |

Filters support these operators:

- `eq` (default), `is_not`
- `in`, `not_in`
- `contains`
- `regex`
- `is_set`
- `gt`, `gte`, `lt`, `lte`

Value comparisons coerce strings, booleans, and numbers when possible. For example, `"21"` can be compared with `18`.

Request fields honored by `/flags` and `/decide`:

- `flag_keys_to_evaluate`
- `evaluation_environments`
- `person_properties`
- `group_properties`
- `groups`

## Bucketing

Rollout bucketing is deterministic:

- Hash: `sha1("{salt}:{hash_id}")`
- `hash_id` is `distinct_id` for person flags, or the group key when `group_type` is set
- Bucket: `hash % 100`
- `salt` defaults to the flag `key` if not provided

Example:

```json
{
  "flags": [
    {
      "key": "pro-flag",
      "active": true,
      "rollout_percentage": 100,
      "id": 12,
      "version": 3,
      "description": "Pro users",
      "salt": "pro-flag-salt",
      "conditions": [
        {
          "properties": [
            { "key": "plan", "value": ["pro", "enterprise"], "operator": "in" },
            { "key": "age", "value": 18, "operator": "gte" }
          ]
        }
      ],
      "payload": { "tier": "pro" }
    }
  ]
}
```

Limitations: cohorts and event-based filters are not supported.

## HMAC Signing

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

`X-HUB-SIGNATURE` with `sha1=` is also accepted for GitHub-style webhook compatibility.

## Enrichment

Hogflare adds Cloudflare request data into `properties` when those keys are not already present:

- `$ip` from `CF-Connecting-IP`
- `$geoip_*` from Cloudflare request metadata
- `cf_*` fields: `cf_asn`, `cf_as_organization`, `cf_colo`, `cf_metro_code`, `cf_ray`
