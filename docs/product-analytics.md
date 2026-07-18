# Product Analytics

Hogflare serves product analytics from `/analytics`. The dashboard uses the repo's Sidemantic `events`, `pageviews`, and `sessions` models through the native Sidemantic/DuckDB Iceberg bridge.

## API Config

Product analytics shares the same R2 Data Catalog warehouse credentials as replay, but uses analytics-specific names when provided:

| Setting | Notes |
| --- | --- |
| `HOGFLARE_ANALYTICS_ACCOUNT_ID` | Cloudflare account id for the R2 Data Catalog warehouse. Falls back to `HOGFLARE_REPLAY_ACCOUNT_ID`. |
| `HOGFLARE_ANALYTICS_BUCKET` | R2 bucket name backing the warehouse. Falls back to `HOGFLARE_REPLAY_BUCKET`. |
| `HOGFLARE_ANALYTICS_R2_SQL_TOKEN` | R2 SQL/Data Catalog token. Store as a secret. Falls back to `HOGFLARE_REPLAY_R2_SQL_TOKEN`. |
| `HOGFLARE_ANALYTICS_EVENTS_TABLE` | Iceberg events table. Falls back to `HOGFLARE_REPLAY_EVENTS_TABLE`. |
| `HOGFLARE_ANALYTICS_PERSONS_TABLE` | Optional Iceberg persons table. Defaults are inferred from the events table. |
| `HOGFLARE_ANALYTICS_MODEL_DIR` | Optional Sidemantic model directory. Defaults to `models`. |
| `HOGFLARE_ANALYTICS_SIDEMANTIC_SCRIPT` | Optional override for the native analytics worker script. |
| `HOGFLARE_ANALYTICS_PREAGG` | Optional Sidemantic pre-aggregation switch. Defaults to enabled. Set to `0` to disable. |
| `HOGFLARE_ANALYTICS_PREAGG_REFRESH` | Optional materialization switch. Defaults to enabled. Set to `0` to query the source models without building local rollups. |
| `HOGFLARE_ANALYTICS_PREAGG_SCHEMA` | Optional DuckDB schema for Sidemantic rollup tables. Defaults to `sidemantic_preagg`. |
| `HOGFLARE_ANALYTICS_PREAGG_DATABASE` | Optional local DuckDB path used to persist pre-aggregation partitions across worker restarts. Defaults to an account-and-bucket-specific file under `/tmp`. |
| `HOGFLARE_ANALYTICS_PREAGG_REFRESH_INTERVAL_SECONDS` | Optional bounded refresh cadence. Defaults to `3600` seconds. |

## Routes

- `/` serves the Hogflare app and opens product analytics by default.
- `/analytics` serves the product analytics view.
- `/analytics/api/charts` returns overview metrics, a focused trend, and semantic breakdowns including domains, referrers, browser, country, region, and city leaderboards.
- `/replay` serves the replay feature.

At worker startup, Sidemantic bootstraps the complete history for known count-based chart shapes. It then refreshes only recent day or month partitions on the configured cadence and automatically routes eligible queries through those tables. The local DuckDB file preserves immutable historical partitions across worker restarts. `metric`, `dimension`, and `granularity` choose the focused chart. `semantic_filters` carries clickable cross-filter state as a JSON object of semantic dimensions to values. Analytics leaderboards use a fixed top-10 row cap plus an Others row.
