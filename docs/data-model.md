# Data Model

## Query Data

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

SELECT count(*) FROM iceberg_catalog.default.hogflare_events;
SELECT count(*) FROM iceberg_catalog.default.hogflare_persons;
SELECT * FROM iceberg_catalog.default.hogflare_persons LIMIT 5;
```

If you used versioned table names during a migration, substitute those names here.

## Event Shape In R2

Each row is a `PipelineEvent` with these columns:

| Field | Type / Notes |
| --- | --- |
| `uuid` | string (UUID v4) |
| `team_id` | int64 (optional) |
| `source` | string |
| `event` | string |
| `distinct_id` | string |
| `timestamp` | RFC3339 timestamp (optional) |
| `created_at` | RFC3339 timestamp |
| `properties` | JSON |
| `context` | JSON |
| `person_id` | string (person UUID) |
| `person_created_at` | RFC3339 timestamp |
| `person_properties` | JSON |
| `group0..group4` | string (group key slots) |
| `group_properties` | JSON by group type |
| `api_key` | string |
| `extra` | JSON |

## Person Shape In R2

Each row is a `PersonPipelineRecord` snapshot with these columns:

| Field | Type / Notes |
| --- | --- |
| `uuid` | string (snapshot UUID v4) |
| `team_id` | int64 (optional) |
| `source` | string |
| `operation` | capture, identify, alias, engage, session_recording |
| `person_id` | string (person UUID) |
| `person_int_id` | int64 |
| `canonical_distinct_id` | string |
| `distinct_ids` | string list / array |
| `created_at` | person creation timestamp |
| `updated_at` | snapshot timestamp |
| `version` | person version |
| `properties` | JSON `$set` properties |
| `properties_set_once` | JSON `$set_once` properties |
| `merged_properties` | JSON merged person properties |
| `api_key` | string |
| `source_event_uuid` | event row UUID that produced the snapshot |
