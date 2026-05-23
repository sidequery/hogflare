# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "duckdb>=1.1",
#   "sidemantic @ git+https://github.com/sidequery/sidemantic",
# ]
# ///

from __future__ import annotations

import json
import os
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any

from sidemantic import SemanticLayer, load_from_directory
from sidemantic.sql.generator import SQLGenerator


SNAPSHOT_EVENT = "$snapshot"
SNAPSHOT_ITEMS_EVENT = "$snapshot_items"
IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*$")
NUMERIC_LITERAL_RE = re.compile(r"^-?\d+(?:\.\d+)?$")
GRANULARITIES = {"hour", "day", "week", "month"}
ANALYTICS_BREAKDOWN_LIMIT = 10
CHART_PANELS = {
    "summary",
    "series",
    "focus_breakdown",
    "top_events",
    "top_pages",
    "domains",
    "referring_domains",
    "referrers",
    "browsers",
    "countries",
    "regions",
    "cities",
}
TIME_DIMENSIONS = {
    "events": "event_time",
    "pageviews": "event_time",
    "sessions": "session_start_at",
}
DEFAULT_DIMENSIONS = {
    "events": "events.event_type",
    "pageviews": "pageviews.pathname",
    "sessions": "sessions.landing_path",
}
BUILTIN_BREAKDOWN_DIMENSIONS = {
    "events.event_type",
    "pageviews.pathname",
    "pageviews.host",
    "pageviews.referrer_domain",
    "pageviews.referrer",
    "events.browser",
    "events.geo_country_code",
    "events.geo_region",
    "events.geo_city",
}
METRICS = {
    "events.event_count": {
        "model": "events",
        "metric": "event_count",
        "label": "Events",
        "display": "count",
        "context_key": "event_count",
    },
    "pageviews.pageviews": {
        "model": "pageviews",
        "metric": "pageviews",
        "label": "Pageviews",
        "display": "count",
        "context_key": "pageviews",
    },
    "events.unique_users": {
        "model": "events",
        "metric": "unique_users",
        "label": "Users",
        "display": "count",
    },
    "sessions.session_count": {
        "model": "sessions",
        "metric": "session_count",
        "label": "SDK sessions",
        "display": "count",
        "context_key": "session_count",
    },
    "sessions.average_session_seconds": {
        "model": "sessions",
        "metric": "average_session_seconds",
        "label": "Avg session",
        "display": "seconds",
    },
    "events.snapshot_events": {
        "model": "events",
        "metric": "snapshot_events",
        "label": "Replay chunks",
        "display": "count",
        "context_key": "recordings",
    },
}
DIMENSIONS = {
    "events.event_type": {
        "model": "events",
        "dimension": "event_type",
        "label": "Event",
        "title": "Events",
    },
    "events.browser": {
        "model": "events",
        "dimension": "browser",
        "label": "Browser",
        "title": "Browsers",
    },
    "events.os": {
        "model": "events",
        "dimension": "os",
        "label": "OS",
        "title": "Operating systems",
    },
    "events.device_type": {
        "model": "events",
        "dimension": "device_type",
        "label": "Device type",
        "title": "Devices",
    },
    "events.host": {
        "model": "events",
        "dimension": "host",
        "label": "Host",
        "title": "Domains",
    },
    "events.referrer_domain": {
        "model": "events",
        "dimension": "referrer_domain",
        "label": "Referring domain",
        "title": "Referring domains",
    },
    "events.referrer": {
        "model": "events",
        "dimension": "referrer",
        "label": "Referrer",
        "title": "Referrers",
    },
    "events.geo_country_code": {
        "model": "events",
        "dimension": "geo_country_code",
        "label": "Country",
        "title": "Countries",
    },
    "events.geo_region": {
        "model": "events",
        "dimension": "geo_region",
        "label": "Region",
        "title": "Regions",
    },
    "events.geo_city": {
        "model": "events",
        "dimension": "geo_city",
        "label": "City",
        "title": "Cities",
    },
    "events.geo_timezone": {
        "model": "events",
        "dimension": "geo_timezone",
        "label": "Timezone",
        "title": "Timezones",
    },
    "events.cf_colo": {
        "model": "events",
        "dimension": "cf_colo",
        "label": "Cloudflare colo",
        "title": "Cloudflare colos",
    },
    "events.cf_asn": {
        "model": "events",
        "dimension": "cf_asn",
        "label": "ASN",
        "title": "ASNs",
        "type": "numeric",
    },
    "events.utm_source": {
        "model": "events",
        "dimension": "utm_source",
        "label": "UTM source",
        "title": "UTM sources",
    },
    "events.utm_campaign": {
        "model": "events",
        "dimension": "utm_campaign",
        "label": "UTM campaign",
        "title": "UTM campaigns",
    },
    "pageviews.pathname": {
        "model": "pageviews",
        "dimension": "pathname",
        "label": "Path",
        "title": "Pages",
    },
    "pageviews.host": {
        "model": "pageviews",
        "dimension": "host",
        "label": "Host",
        "title": "Domains",
    },
    "pageviews.referrer_domain": {
        "model": "pageviews",
        "dimension": "referrer_domain",
        "label": "Referring domain",
        "title": "Referring domains",
    },
    "pageviews.referrer": {
        "model": "pageviews",
        "dimension": "referrer",
        "label": "Referrer",
        "title": "Referrers",
    },
    "pageviews.browser": {
        "model": "pageviews",
        "dimension": "browser",
        "label": "Browser",
        "title": "Pageview browsers",
    },
    "pageviews.os": {
        "model": "pageviews",
        "dimension": "os",
        "label": "OS",
        "title": "Pageview OS",
    },
    "pageviews.device_type": {
        "model": "pageviews",
        "dimension": "device_type",
        "label": "Device type",
        "title": "Pageview devices",
    },
    "pageviews.geo_country_code": {
        "model": "pageviews",
        "dimension": "geo_country_code",
        "label": "Country",
        "title": "Pageview countries",
    },
    "pageviews.geo_region": {
        "model": "pageviews",
        "dimension": "geo_region",
        "label": "Region",
        "title": "Pageview regions",
    },
    "pageviews.geo_city": {
        "model": "pageviews",
        "dimension": "geo_city",
        "label": "City",
        "title": "Pageview cities",
    },
    "pageviews.geo_timezone": {
        "model": "pageviews",
        "dimension": "geo_timezone",
        "label": "Timezone",
        "title": "Pageview timezones",
    },
    "pageviews.cf_colo": {
        "model": "pageviews",
        "dimension": "cf_colo",
        "label": "Cloudflare colo",
        "title": "Pageview Cloudflare colos",
    },
    "pageviews.cf_asn": {
        "model": "pageviews",
        "dimension": "cf_asn",
        "label": "ASN",
        "title": "Pageview ASNs",
        "type": "numeric",
    },
    "pageviews.utm_source": {
        "model": "pageviews",
        "dimension": "utm_source",
        "label": "UTM source",
        "title": "UTM sources",
    },
    "pageviews.utm_campaign": {
        "model": "pageviews",
        "dimension": "utm_campaign",
        "label": "UTM campaign",
        "title": "UTM campaigns",
    },
    "sessions.landing_path": {
        "model": "sessions",
        "dimension": "landing_path",
        "label": "Landing path",
        "title": "Landing paths",
    },
    "sessions.browser": {
        "model": "sessions",
        "dimension": "browser",
        "label": "Browser",
        "title": "Session browsers",
    },
    "sessions.os": {
        "model": "sessions",
        "dimension": "os",
        "label": "OS",
        "title": "Session OS",
    },
    "sessions.device_type": {
        "model": "sessions",
        "dimension": "device_type",
        "label": "Device type",
        "title": "Session devices",
    },
    "sessions.referrer_domain": {
        "model": "sessions",
        "dimension": "referrer_domain",
        "label": "Referring domain",
        "title": "Session referring domains",
    },
    "sessions.referrer": {
        "model": "sessions",
        "dimension": "referrer",
        "label": "Referrer",
        "title": "Session referrers",
    },
    "sessions.geo_country_code": {
        "model": "sessions",
        "dimension": "geo_country_code",
        "label": "Country",
        "title": "Session countries",
    },
    "sessions.geo_region": {
        "model": "sessions",
        "dimension": "geo_region",
        "label": "Region",
        "title": "Session regions",
    },
    "sessions.geo_city": {
        "model": "sessions",
        "dimension": "geo_city",
        "label": "City",
        "title": "Session cities",
    },
    "sessions.geo_timezone": {
        "model": "sessions",
        "dimension": "geo_timezone",
        "label": "Timezone",
        "title": "Session timezones",
    },
    "sessions.cf_colo": {
        "model": "sessions",
        "dimension": "cf_colo",
        "label": "Cloudflare colo",
        "title": "Session Cloudflare colos",
    },
    "sessions.cf_asn": {
        "model": "sessions",
        "dimension": "cf_asn",
        "label": "ASN",
        "title": "Session ASNs",
        "type": "numeric",
    },
    "sessions.utm_source": {
        "model": "sessions",
        "dimension": "utm_source",
        "label": "UTM source",
        "title": "Session UTM sources",
    },
}


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "--serve":
        serve()
        return

    query = json.loads(sys.argv[1]) if len(sys.argv) > 1 else json.load(sys.stdin)
    config = AnalyticsConfig.from_env()
    runner = SidemanticAnalytics(config)
    print(json.dumps(runner.run(query), separators=(",", ":")))


def serve() -> None:
    config = AnalyticsConfig.from_env()
    runner = SidemanticAnalytics(config)
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        request_id = None
        try:
            query = json.loads(line)
            request_id = query.get("_request_id")
            response = {"ok": True, "request_id": request_id, "result": runner.run(query)}
        except Exception as exc:
            response = {"ok": False, "request_id": request_id, "error": str(exc)}
        print(json.dumps(response, separators=(",", ":")), flush=True)


class AnalyticsConfig:
    def __init__(
        self,
        *,
        account_id: str,
        bucket: str,
        token: str,
        events_table: str,
        persons_table: str,
        model_dir: Path,
        preagg_enabled: bool,
        preagg_schema: str,
    ) -> None:
        self.account_id = account_id
        self.bucket = bucket
        self.token = token
        self.events_table = events_table
        self.persons_table = persons_table
        self.model_dir = model_dir
        self.preagg_enabled = preagg_enabled
        self.preagg_schema = preagg_schema

    @classmethod
    def from_env(cls) -> "AnalyticsConfig":
        events_table = env_required_any("HOGFLARE_ANALYTICS_EVENTS_TABLE", "HOGFLARE_REPLAY_EVENTS_TABLE")
        persons_table = (
            os.environ.get("HOGFLARE_ANALYTICS_PERSONS_TABLE")
            or infer_persons_table(events_table)
        )
        for name, value in {
            "HOGFLARE_ANALYTICS_EVENTS_TABLE": events_table,
            "HOGFLARE_ANALYTICS_PERSONS_TABLE": persons_table,
        }.items():
            if not IDENTIFIER_RE.match(value):
                raise ValueError(f"{name} must be a dotted SQL identifier")
        preagg_schema = (
            os.environ.get("HOGFLARE_ANALYTICS_PREAGG_SCHEMA")
            or "sidemantic_preagg"
        )
        if not IDENTIFIER_RE.match(preagg_schema):
            raise ValueError("HOGFLARE_ANALYTICS_PREAGG_SCHEMA must be a SQL identifier")

        return cls(
            account_id=env_required_any("HOGFLARE_ANALYTICS_ACCOUNT_ID", "HOGFLARE_REPLAY_ACCOUNT_ID"),
            bucket=env_required_any("HOGFLARE_ANALYTICS_BUCKET", "HOGFLARE_REPLAY_BUCKET"),
            token=env_required_any(
                "HOGFLARE_ANALYTICS_R2_SQL_TOKEN",
                "HOGFLARE_REPLAY_R2_SQL_TOKEN",
            ),
            events_table=events_table,
            persons_table=persons_table,
            model_dir=Path(
                os.environ.get("HOGFLARE_ANALYTICS_MODEL_DIR")
                or "models"
            ),
            preagg_enabled=env_flag("HOGFLARE_ANALYTICS_PREAGG", default=True),
            preagg_schema=preagg_schema,
        )

    @property
    def warehouse_name(self) -> str:
        return f"{self.account_id}_{self.bucket}"

    @property
    def catalog_endpoint(self) -> str:
        return f"https://catalog.cloudflarestorage.com/{self.account_id}/{self.bucket}"

    @property
    def attached_events_table(self) -> str:
        return f"iceberg_catalog.{self.events_table}"

    @property
    def attached_persons_table(self) -> str:
        return f"iceberg_catalog.{self.persons_table}"


class SidemanticAnalytics:
    def __init__(self, config: AnalyticsConfig) -> None:
        self.config = config
        self.layer = SemanticLayer(
            connection="duckdb:///:memory:",
            auto_register=False,
            use_preaggregations=config.preagg_enabled,
            preagg_schema=config.preagg_schema,
        )
        self._connect_iceberg()
        load_from_directory(self.layer, str(config.model_dir))
        self._bind_model_tables()
        self._materialize_preaggregations()
        self.generator = SQLGenerator(
            self.layer.graph,
            dialect="duckdb",
            preagg_schema=config.preagg_schema,
        )

    def run(self, query: dict[str, Any]) -> dict[str, Any]:
        top_limit = ANALYTICS_BREAKDOWN_LIMIT
        metric = metric_def(query.get("metric"))
        dimension = dimension_def(query.get("dimension"), metric["model"])
        granularity = granularity_for(query.get("granularity"))
        events_filters = self._filters_for("events", query)
        pageviews_filters = self._filters_for("pageviews", query)
        sessions_filters = self._filters_for("sessions", query)
        focus_filters = self._filters_for(metric["model"], query)
        panel = clean(query.get("panel"))
        if panel in CHART_PANELS:
            return self._run_panel(
                panel=panel,
                query=query,
                top_limit=top_limit,
                metric=metric,
                dimension=dimension,
                granularity=granularity,
                events_filters=events_filters,
                pageviews_filters=pageviews_filters,
                sessions_filters=sessions_filters,
                focus_filters=focus_filters,
            )

        events_summary = self._one(
            metrics=["events.event_count", "events.unique_users", "events.snapshot_events"],
            filters=events_filters,
            skip_default_time_dimensions=True,
        )
        pageviews_summary = self._one(
            metrics=["pageviews.pageviews"],
            filters=pageviews_filters,
            skip_default_time_dimensions=True,
        )
        sessions_summary = self._one(
            metrics=["sessions.session_count", "sessions.average_session_seconds"],
            filters=sessions_filters,
            skip_default_time_dimensions=True,
        )
        context_series_rows = self._rows(
            metrics=[
                "events.event_count",
                "events.pageviews",
                "events.unique_sessions",
                "events.snapshot_events",
            ],
            dimensions=[f"events.event_time__{granularity}"],
            filters=events_filters,
            order_by=[f"events.event_time__{granularity}"],
        )
        focus_time_dimension = TIME_DIMENSIONS[metric["model"]]
        focus_bucket_ref = f"{metric['model']}.{focus_time_dimension}__{granularity}"
        focus_series_rows = self._rows(
            metrics=[metric["ref"]],
            dimensions=[focus_bucket_ref],
            filters=focus_filters,
            order_by=[focus_bucket_ref],
        )
        focus_total = self._one(
            metrics=[metric["ref"]],
            filters=focus_filters,
            skip_default_time_dimensions=True,
        )
        focus_breakdown_filters = [
            *self._filters_for(metric["model"], query, exclude_dimension_ref=dimension["ref"]),
            f"{dimension['ref']} is not null",
        ]
        focus_breakdown_rows = self._rows(
            metrics=[metric["ref"]],
            dimensions=[dimension["ref"]],
            filters=focus_breakdown_filters,
            order_by=[f"{metric['ref']} DESC"],
            limit=top_limit,
            skip_default_time_dimensions=True,
        )
        focus_breakdown_total = self._metric_total(
            metric["ref"],
            metric["metric"],
            focus_breakdown_filters,
        )
        top_events = self._rows(
            metrics=["events.event_count"],
            dimensions=["events.event_type"],
            filters=[
                *self._filters_for("events", query, exclude_dimension_ref="events.event_type"),
                "events.event_type is not null",
            ],
            order_by=["events.event_count DESC"],
            limit=top_limit,
            skip_default_time_dimensions=True,
        )
        top_pages = self._rows(
            metrics=["pageviews.pageviews"],
            dimensions=["pageviews.pathname"],
            filters=[
                *self._filters_for("pageviews", query, exclude_dimension_ref="pageviews.pathname"),
                "pageviews.pathname is not null",
            ],
            order_by=["pageviews.pageviews DESC"],
            limit=top_limit,
            skip_default_time_dimensions=True,
        )
        domains = self._rows(
            metrics=["pageviews.pageviews"],
            dimensions=["pageviews.host"],
            filters=[
                *self._filters_for("pageviews", query, exclude_dimension_ref="pageviews.host"),
                "pageviews.host is not null",
            ],
            order_by=["pageviews.pageviews DESC"],
            limit=top_limit,
            skip_default_time_dimensions=True,
        )
        referring_domains = self._rows(
            metrics=["pageviews.pageviews"],
            dimensions=["pageviews.referrer_domain"],
            filters=[
                *self._filters_for("pageviews", query, exclude_dimension_ref="pageviews.referrer_domain"),
                "pageviews.referrer_domain is not null",
            ],
            order_by=["pageviews.pageviews DESC"],
            limit=top_limit,
            skip_default_time_dimensions=True,
        )
        referrers = self._rows(
            metrics=["pageviews.pageviews"],
            dimensions=["pageviews.referrer"],
            filters=[
                *self._filters_for("pageviews", query, exclude_dimension_ref="pageviews.referrer"),
                "pageviews.referrer is not null",
            ],
            order_by=["pageviews.pageviews DESC"],
            limit=top_limit,
            skip_default_time_dimensions=True,
        )
        browsers = self._rows(
            metrics=["events.event_count"],
            dimensions=["events.browser"],
            filters=[
                *self._filters_for("events", query, exclude_dimension_ref="events.browser"),
                "events.browser is not null",
            ],
            order_by=["events.event_count DESC"],
            limit=top_limit,
            skip_default_time_dimensions=True,
        )
        countries = self._rows(
            metrics=["events.event_count"],
            dimensions=["events.geo_country_code"],
            filters=[
                *self._filters_for("events", query, exclude_dimension_ref="events.geo_country_code"),
                "events.geo_country_code is not null",
            ],
            order_by=["events.event_count DESC"],
            limit=top_limit,
            skip_default_time_dimensions=True,
        )
        regions = self._rows(
            metrics=["events.event_count"],
            dimensions=["events.geo_region"],
            filters=[
                *self._filters_for("events", query, exclude_dimension_ref="events.geo_region"),
                "events.geo_region is not null",
            ],
            order_by=["events.event_count DESC"],
            limit=top_limit,
            skip_default_time_dimensions=True,
        )
        cities = self._rows(
            metrics=["events.event_count"],
            dimensions=["events.geo_city"],
            filters=[
                *self._filters_for("events", query, exclude_dimension_ref="events.geo_city"),
                "events.geo_city is not null",
            ],
            order_by=["events.event_count DESC"],
            limit=top_limit,
            skip_default_time_dimensions=True,
        )

        event_count = to_int(events_summary.get("event_count"))
        pageviews = to_int(pageviews_summary.get("pageviews"))
        unique_users = to_int(events_summary.get("unique_users"))
        session_count = to_int(sessions_summary.get("session_count"))
        average_session_seconds = to_float(sessions_summary.get("average_session_seconds"))
        replay_chunks = to_int(events_summary.get("snapshot_events"))
        focus_value = to_float(focus_total.get(metric["metric"]))

        breakdowns = []
        if not use_builtin_breakdown(dimension):
            breakdowns.append(
                breakdown(
                    dimension.get("title", dimension["label"]),
                    dimension["model"],
                    dimension["dimension"],
                    metric["ref"],
                    focus_breakdown_rows,
                    dimension["dimension"],
                    metric["metric"],
                    focus_breakdown_total,
                )
            )
        for candidate in [
            breakdown(
                "Top events",
                "events",
                "event_type",
                "events.event_count",
                top_events,
                "event_type",
                "event_count",
                event_count,
            ),
            breakdown(
                "Top pages",
                "pageviews",
                "pathname",
                "pageviews.pageviews",
                top_pages,
                "pathname",
                "pageviews",
                pageviews,
            ),
            breakdown(
                "Domains",
                "pageviews",
                "host",
                "pageviews.pageviews",
                domains,
                "host",
                "pageviews",
                pageviews,
            ),
            breakdown(
                "Referring domains",
                "pageviews",
                "referrer_domain",
                "pageviews.pageviews",
                referring_domains,
                "referrer_domain",
                "pageviews",
                pageviews,
            ),
            breakdown(
                "Referrers",
                "pageviews",
                "referrer",
                "pageviews.pageviews",
                referrers,
                "referrer",
                "pageviews",
                pageviews,
            ),
            breakdown(
                "Browsers",
                "events",
                "browser",
                "events.event_count",
                browsers,
                "browser",
                "event_count",
                event_count,
            ),
            breakdown(
                "Countries",
                "events",
                "geo_country_code",
                "events.event_count",
                countries,
                "geo_country_code",
                "event_count",
                event_count,
            ),
            breakdown(
                "Regions",
                "events",
                "geo_region",
                "events.event_count",
                regions,
                "geo_region",
                "event_count",
                event_count,
            ),
            breakdown(
                "Cities",
                "events",
                "geo_city",
                "events.event_count",
                cities,
                "geo_city",
                "event_count",
                event_count,
            ),
        ]:
            if not any(same_breakdown(candidate, existing) for existing in breakdowns):
                breakdowns.append(candidate)

        return {
            "focus": {
                "metric": metric["ref"],
                "metric_label": metric["label"],
                "dimension": dimension["ref"],
                "dimension_label": dimension["label"],
                "granularity": granularity,
            },
            "summary": [
                count_metric("Events", event_count, "events", "event_count"),
                count_metric("Pageviews", pageviews, "pageviews", "pageviews"),
                count_metric("Users", unique_users, "events", "unique_users"),
                count_metric("SDK sessions", session_count, "sessions", "session_count"),
                seconds_metric(
                    "Avg session",
                    average_session_seconds,
                    "sessions",
                    "average_session_seconds",
                ),
                count_metric("Replay chunks", replay_chunks, "events", "snapshot_events"),
            ],
            "series": series_points(
                context_series_rows,
                focus_series_rows,
                focus_time_dimension,
                granularity,
                metric["metric"],
            ),
            "breakdowns": breakdowns,
        }

    def _run_panel(
        self,
        *,
        panel: str,
        query: dict[str, Any],
        top_limit: int,
        metric: dict[str, Any],
        dimension: dict[str, Any],
        granularity: str,
        events_filters: list[str],
        pageviews_filters: list[str],
        sessions_filters: list[str],
        focus_filters: list[str],
    ) -> dict[str, Any]:
        response = self._empty_response(metric, dimension, granularity)

        if panel == "summary":
            events_summary = self._one(
                metrics=["events.event_count", "events.unique_users", "events.snapshot_events"],
                filters=events_filters,
                skip_default_time_dimensions=True,
            )
            pageviews_summary = self._one(
                metrics=["pageviews.pageviews"],
                filters=pageviews_filters,
                skip_default_time_dimensions=True,
            )
            sessions_summary = self._one(
                metrics=["sessions.session_count", "sessions.average_session_seconds"],
                filters=sessions_filters,
                skip_default_time_dimensions=True,
            )
            response["summary"] = [
                count_metric("Events", to_int(events_summary.get("event_count")), "events", "event_count"),
                count_metric("Pageviews", to_int(pageviews_summary.get("pageviews")), "pageviews", "pageviews"),
                count_metric("Users", to_int(events_summary.get("unique_users")), "events", "unique_users"),
                count_metric("SDK sessions", to_int(sessions_summary.get("session_count")), "sessions", "session_count"),
                seconds_metric(
                    "Avg session",
                    to_float(sessions_summary.get("average_session_seconds")),
                    "sessions",
                    "average_session_seconds",
                ),
                count_metric("Replay chunks", to_int(events_summary.get("snapshot_events")), "events", "snapshot_events"),
            ]
            return response

        if panel == "series":
            context_series_rows = self._rows(
                metrics=[
                    "events.event_count",
                    "events.pageviews",
                    "events.unique_sessions",
                    "events.snapshot_events",
                ],
                dimensions=[f"events.event_time__{granularity}"],
                filters=events_filters,
                order_by=[f"events.event_time__{granularity}"],
            )
            focus_time_dimension = TIME_DIMENSIONS[metric["model"]]
            focus_bucket_ref = f"{metric['model']}.{focus_time_dimension}__{granularity}"
            focus_series_rows = self._rows(
                metrics=[metric["ref"]],
                dimensions=[focus_bucket_ref],
                filters=focus_filters,
                order_by=[focus_bucket_ref],
            )
            response["series"] = series_points(
                context_series_rows,
                focus_series_rows,
                focus_time_dimension,
                granularity,
                metric["metric"],
            )
            return response

        if panel == "focus_breakdown":
            if use_builtin_breakdown(dimension):
                return response
            focus_breakdown_filters = [
                *self._filters_for(metric["model"], query, exclude_dimension_ref=dimension["ref"]),
                f"{dimension['ref']} is not null",
            ]
            focus_breakdown_rows = self._rows(
                metrics=[metric["ref"]],
                dimensions=[dimension["ref"]],
                filters=focus_breakdown_filters,
                order_by=[f"{metric['ref']} DESC"],
                limit=top_limit,
                skip_default_time_dimensions=True,
            )
            focus_breakdown_total = self._metric_total(
                metric["ref"],
                metric["metric"],
                focus_breakdown_filters,
            )
            response["breakdowns"] = [
                breakdown(
                    dimension.get("title", dimension["label"]),
                    dimension["model"],
                    dimension["dimension"],
                    metric["ref"],
                    focus_breakdown_rows,
                    dimension["dimension"],
                    metric["metric"],
                    focus_breakdown_total,
                )
            ]
            return response

        leaderboard = self._leaderboard_panel(panel, query, top_limit)
        if leaderboard:
            response["breakdowns"] = [leaderboard]
        return response

    def _leaderboard_panel(
        self,
        panel: str,
        query: dict[str, Any],
        top_limit: int,
    ) -> dict[str, Any] | None:
        configs = {
            "top_events": ("Top events", "events", "event_type", "events.event_count", "event_count"),
            "top_pages": ("Top pages", "pageviews", "pathname", "pageviews.pageviews", "pageviews"),
            "domains": ("Domains", "pageviews", "host", "pageviews.pageviews", "pageviews"),
            "referring_domains": (
                "Referring domains",
                "pageviews",
                "referrer_domain",
                "pageviews.pageviews",
                "pageviews",
            ),
            "referrers": ("Referrers", "pageviews", "referrer", "pageviews.pageviews", "pageviews"),
            "browsers": ("Browsers", "events", "browser", "events.event_count", "event_count"),
            "countries": ("Countries", "events", "geo_country_code", "events.event_count", "event_count"),
            "regions": ("Regions", "events", "geo_region", "events.event_count", "event_count"),
            "cities": ("Cities", "events", "geo_city", "events.event_count", "event_count"),
        }
        config = configs.get(panel)
        if not config:
            return None
        title, model, dimension, metric_ref, value_key = config
        dimension_ref = f"{model}.{dimension}"
        filters = [
            *self._filters_for(model, query, exclude_dimension_ref=dimension_ref),
            f"{dimension_ref} is not null",
        ]
        rows = self._rows(
            metrics=[metric_ref],
            dimensions=[dimension_ref],
            filters=filters,
            order_by=[f"{metric_ref} DESC"],
            limit=top_limit,
            skip_default_time_dimensions=True,
        )
        total = self._metric_total(metric_ref, value_key, filters)
        return breakdown(title, model, dimension, metric_ref, rows, dimension, value_key, total)

    @staticmethod
    def _empty_response(metric: dict[str, Any], dimension: dict[str, Any], granularity: str) -> dict[str, Any]:
        return {
            "focus": {
                "metric": metric["ref"],
                "metric_label": metric["label"],
                "dimension": dimension["ref"],
                "dimension_label": dimension["label"],
                "granularity": granularity,
            },
            "summary": [],
            "series": [],
            "breakdowns": [],
        }

    def _connect_iceberg(self) -> None:
        con = self.layer.adapter.raw_connection
        con.execute("INSTALL httpfs")
        con.execute("INSTALL iceberg")
        con.execute("LOAD httpfs")
        con.execute("LOAD iceberg")
        con.execute("CREATE OR REPLACE SECRET r2_catalog_secret (TYPE ICEBERG, TOKEN ?)", [self.config.token])
        con.execute(
            f"ATTACH '{self.config.warehouse_name}' AS iceberg_catalog "
            f"(TYPE ICEBERG, ENDPOINT '{self.config.catalog_endpoint}')"
        )

    def _bind_model_tables(self) -> None:
        for model in self.layer.graph.models.values():
            if model.sql:
                model.sql = (
                    model.sql.replace("{{ events_table }}", self.config.attached_events_table)
                    .replace("{{ persons_table }}", self.config.attached_persons_table)
                )

    def _rows(
        self,
        *,
        metrics: list[str],
        dimensions: list[str] | None = None,
        filters: list[str] | None = None,
        order_by: list[str] | None = None,
        limit: int | None = None,
        skip_default_time_dimensions: bool = False,
    ) -> list[dict[str, Any]]:
        use_preaggregations = self.config.preagg_enabled
        sql = self.generator.generate(
            metrics=metrics,
            dimensions=dimensions or [],
            filters=filters or [],
            order_by=order_by,
            limit=limit,
            skip_default_time_dimensions=skip_default_time_dimensions,
            use_preaggregations=use_preaggregations,
        )
        try:
            relation = self.layer.adapter.execute(sql)
        except Exception:
            if not use_preaggregations:
                raise
            fallback_sql = self.generator.generate(
                metrics=metrics,
                dimensions=dimensions or [],
                filters=filters or [],
                order_by=order_by,
                limit=limit,
                skip_default_time_dimensions=skip_default_time_dimensions,
                use_preaggregations=False,
            )
            relation = self.layer.adapter.execute(fallback_sql)
        columns = [column[0] for column in relation.description]
        return [dict(zip(columns, row, strict=False)) for row in relation.fetchall()]

    def _materialize_preaggregations(self) -> None:
        if not self.config.preagg_enabled:
            return
        con = self.layer.adapter.raw_connection
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {self.config.preagg_schema}")
        for model_name, model in self.layer.graph.models.items():
            for preagg in model.pre_aggregations:
                table_name = preagg.get_table_name(model_name, schema=self.config.preagg_schema)
                source_sql = preagg.generate_materialization_sql(model)
                started = time.perf_counter()
                print(
                    f"sidemantic preagg refresh start {model_name}.{preagg.name} -> {table_name}",
                    file=sys.stderr,
                    flush=True,
                )
                result = preagg.refresh(
                    connection=con,
                    source_sql=source_sql,
                    table_name=table_name,
                    mode="full",
                )
                con.execute(f"ANALYZE {table_name}")
                elapsed = time.perf_counter() - started
                print(
                    "sidemantic preagg refresh done "
                    f"{model_name}.{preagg.name} rows={result.rows_inserted} "
                    f"seconds={elapsed:.2f}",
                    file=sys.stderr,
                    flush=True,
                )

    def _one(self, **kwargs: Any) -> dict[str, Any]:
        rows = self._rows(**kwargs)
        return rows[0] if rows else {}

    def _metric_total(self, metric_ref: str, value_key: str, filters: list[str]) -> float:
        return to_float(
            self._one(
                metrics=[metric_ref],
                filters=filters,
                skip_default_time_dimensions=True,
            ).get(value_key)
        )

    def _filters_for(
        self,
        model: str,
        query: dict[str, Any],
        *,
        exclude_dimension_ref: str | None = None,
    ) -> list[str]:
        fields = {
            "events": {
                "distinct_id": "distinct_id",
                "session_id": "session_id",
                "event_name": "event_type",
                "url": "current_url",
                "time": "event_time",
            },
            "pageviews": {
                "distinct_id": "actor_id",
                "session_id": "session_id",
                "event_name": "event_type",
                "url": "current_url",
                "time": "event_time",
            },
            "sessions": {
                "distinct_id": "actor_id",
                "session_id": "session_id",
                "url": "landing_url",
                "time": "session_start_at",
            },
        }[model]
        filters: list[str] = []

        for query_key in ("distinct_id", "session_id", "event_name"):
            value = clean(query.get(query_key))
            field = fields.get(query_key)
            if value and field:
                filters.append(f"{model}.{field} = {sql_string(value)}")

        url = clean(query.get("url"))
        if url and fields.get("url"):
            needle = sql_like(url.lower())
            if model == "sessions":
                filters.append(
                    f"(lower({model}.landing_url) like {needle} or lower({model}.exit_url) like {needle})"
                )
            else:
                filters.append(f"lower({model}.{fields['url']}) like {needle}")

        date_from = clean(query.get("date_from"))
        if date_from:
            filters.append(f"{model}.{fields['time']} >= {sql_string(date_from)}")
        date_to = clean(query.get("date_to"))
        if date_to:
            filters.append(f"{model}.{fields['time']} <= {sql_string(date_to)}")

        filters.extend(semantic_filters_for(model, query, exclude_dimension_ref=exclude_dimension_ref))
        return filters


def metric_def(value: Any) -> dict[str, Any]:
    ref = clean(value) or "events.event_count"
    if ref not in METRICS:
        ref = "events.event_count"
    metric = dict(METRICS[ref])
    metric["ref"] = ref
    return metric


def dimension_def(value: Any, model: str) -> dict[str, Any]:
    ref = clean(value) or DEFAULT_DIMENSIONS[model]
    if ref not in DIMENSIONS or DIMENSIONS[ref]["model"] != model:
        ref = DEFAULT_DIMENSIONS[model]
    dimension = dict(DIMENSIONS[ref])
    dimension["ref"] = ref
    return dimension


def granularity_for(value: Any) -> str:
    granularity = clean(value) or "day"
    return granularity if granularity in GRANULARITIES else "day"


def semantic_filters_for(
    model: str,
    query: dict[str, Any],
    *,
    exclude_dimension_ref: str | None = None,
) -> list[str]:
    filters: list[str] = []
    for source_ref, values in semantic_filters_from_query(query).items():
        if source_ref == exclude_dimension_ref:
            continue
        target_ref = dimension_ref_for_model(source_ref, model)
        if not target_ref:
            continue
        if target_ref == exclude_dimension_ref:
            continue
        target = DIMENSIONS[target_ref]
        sql_values = []
        for value in values:
            sql_value = value_sql(target, value)
            if sql_value is not None:
                sql_values.append(sql_value)
        if not sql_values:
            continue
        if len(sql_values) == 1:
            filters.append(f"{target_ref} = {sql_values[0]}")
        else:
            filters.append(f"{target_ref} in ({', '.join(sql_values)})")
    return filters


def semantic_filters_from_query(query: dict[str, Any]) -> dict[str, list[str]]:
    raw = clean(query.get("semantic_filters"))
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if not isinstance(parsed, dict):
        return {}

    filters: dict[str, list[str]] = {}
    for ref, values in parsed.items():
        ref = clean(ref)
        if ref not in DIMENSIONS:
            continue
        value_list = values if isinstance(values, list) else [values]
        cleaned = [value for value in (clean(value) for value in value_list) if value is not None]
        if cleaned:
            filters[ref] = cleaned[:20]
    return filters


def dimension_ref_for_model(source_ref: str, model: str) -> str | None:
    source = DIMENSIONS.get(source_ref)
    if not source:
        return None
    if source["model"] == model:
        return source_ref
    source_dimension = source["dimension"]
    for ref, candidate in DIMENSIONS.items():
        if candidate["model"] == model and candidate["dimension"] == source_dimension:
            return ref
    return None


def series_points(
    context_rows: list[dict[str, Any]],
    focus_rows: list[dict[str, Any]],
    focus_time_dimension: str,
    granularity: str,
    focus_value_key: str,
) -> list[dict[str, Any]]:
    context_bucket_key = f"event_time__{granularity}"
    focus_bucket_key = f"{focus_time_dimension}__{granularity}"
    context_by_bucket = {
        date_label(row.get(context_bucket_key)): row
        for row in context_rows
        if row.get(context_bucket_key) is not None
    }
    focus_by_bucket = {
        date_label(row.get(focus_bucket_key)): row
        for row in focus_rows
        if row.get(focus_bucket_key) is not None
    }
    buckets = sorted(set(context_by_bucket) | set(focus_by_bucket))
    return [
        {
            "bucket": bucket,
            "event_count": to_int(context_by_bucket.get(bucket, {}).get("event_count")),
            "pageviews": to_int(context_by_bucket.get(bucket, {}).get("pageviews")),
            "session_count": to_int(context_by_bucket.get(bucket, {}).get("unique_sessions")),
            "recordings": to_int(context_by_bucket.get(bucket, {}).get("snapshot_events")),
            "focused_value": to_float(focus_by_bucket.get(bucket, {}).get(focus_value_key)),
        }
        for bucket in buckets
    ]


def same_breakdown(left: dict[str, Any], right: dict[str, Any]) -> bool:
    return (
        left.get("model") == right.get("model")
        and left.get("dimension") == right.get("dimension")
        and left.get("metric") == right.get("metric")
    )


def use_geo_hierarchy_breakdown(dimension: dict[str, Any]) -> bool:
    return dimension.get("dimension") in {"geo_country_code", "geo_region", "geo_city"}


def use_builtin_breakdown(dimension: dict[str, Any]) -> bool:
    return dimension.get("ref") in BUILTIN_BREAKDOWN_DIMENSIONS or use_geo_hierarchy_breakdown(dimension)


def breakdown(
    title: str,
    model: str,
    dimension: str,
    metric: str,
    rows: list[dict[str, Any]],
    label_key: str,
    value_key: str,
    total: float,
    *,
    denominator: str = "max",
) -> dict[str, Any]:
    row_values = [
        (str(row.get(label_key)), to_float(row.get(value_key)))
        for row in rows
        if row.get(label_key) is not None
    ]
    values = [value for _, value in row_values]
    percent_base = max(values, default=0.0) if denominator == "max" else total
    payload_rows = [
        {
            "label": label,
            "value": value,
            "percent": percent(value, percent_base),
        }
        for label, value in row_values
    ]
    other_value = max(0.0, total - sum(values))
    if other_value > 0.0001:
        payload_rows.append(
            {
                "label": "Others",
                "value": other_value,
                "percent": percent(other_value, percent_base),
                "is_other": True,
            }
        )
    return {
        "title": title,
        "model": model,
        "dimension": dimension,
        "metric": metric,
        "rows": payload_rows,
    }


def count_metric(label: str, value: int, model: str, metric: str) -> dict[str, Any]:
    return {
        "label": label,
        "value": float(value),
        "display_value": format_count(value),
        "model": model,
        "metric": metric,
        "semantic_ref": f"{model}.{metric}",
    }


def seconds_metric(label: str, value: float, model: str, metric: str) -> dict[str, Any]:
    return {
        "label": label,
        "value": value,
        "display_value": f"{value:.1f}s",
        "model": model,
        "metric": metric,
        "semantic_ref": f"{model}.{metric}",
    }


def infer_persons_table(events_table: str) -> str:
    replacements = [
        ("hogflare_events_v3", "hogflare_persons_v2"),
        ("hogflare_events", "hogflare_persons"),
        ("events", "persons"),
    ]
    for needle, replacement in replacements:
        if needle in events_table:
            return events_table.replace(needle, replacement)
    return "default.hogflare_persons_v2"


def clean(value: Any) -> str | None:
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def value_sql(dimension: dict[str, Any], value: str) -> str | None:
    normalized = clean(value)
    if normalized is None:
        return None
    if dimension.get("type") == "numeric":
        return normalized if NUMERIC_LITERAL_RE.match(normalized) else None
    return sql_string(normalized)


def sql_like(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_").replace("'", "''")
    return f"'%{escaped}%'"


def to_int(value: Any) -> int:
    return int(value or 0)


def to_float(value: Any) -> float:
    return float(value or 0)


def percent(value: float, total: float) -> float:
    return 0.0 if total == 0 else (value / total) * 100.0


def format_count(value: int) -> str:
    return f"{value:,}"


def date_label(value: Any) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def clamp_int(value: Any, *, default: int, low: int, high: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return min(high, max(low, parsed))


def env_required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def env_required_any(*names: str) -> str:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    raise RuntimeError(f"{names[0]} is required")


def env_flag(name: str, *, default: bool) -> bool:
    value = clean(os.environ.get(name))
    if value is None:
        return default
    return value.lower() not in {"0", "false", "no", "off"}


if __name__ == "__main__":
    main()
