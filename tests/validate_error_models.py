#!/usr/bin/env python3

import json
from pathlib import Path

import duckdb
import yaml


ROOT = Path(__file__).resolve().parents[1]


def model_sql(name: str) -> str:
    document = yaml.safe_load((ROOT / "models" / f"{name}.yml").read_text())
    return (
        document["models"][0]["sql"]
        .replace("{{ events_table }}", "events")
        .replace("{{ persons_table }}", "persons")
    )


def main() -> None:
    connection = duckdb.connect(":memory:")
    connection.execute(
        """
        create table events (
          uuid varchar,
          team_id bigint,
          source varchar,
          event varchar,
          distinct_id varchar,
          person_id varchar,
          created_at timestamp,
          timestamp timestamp,
          properties json,
          context json,
          group0 varchar,
          group1 varchar,
          group2 varchar,
          group3 varchar,
          group4 varchar,
          api_key varchar
        )
        """
    )
    connection.execute(
        """
        create table persons (
          person_id varchar,
          canonical_distinct_id varchar,
          distinct_ids varchar[],
          created_at timestamp,
          updated_at timestamp,
          version bigint,
          team_id bigint,
          api_key varchar
        )
        """
    )
    connection.executemany(
        "insert into persons values (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "person-a",
                "identified-a",
                ["user-a", "identified-a"],
                "2026-07-18 09:00:00",
                "2026-07-18 10:04:00",
                2,
                1,
                "phc_project_a",
            ),
            (
                "other-project-person",
                "user-a",
                ["user-a"],
                "2026-07-18 09:00:00",
                "2026-07-18 10:05:00",
                3,
                2,
                "phc_project_b",
            ),
        ],
    )

    frames_a = [
        {"function": "runtime", "filename": "runtime.js", "lineno": 1},
        {"function": "checkoutA", "filename": "checkout-a.js", "lineno": 42},
    ]
    frames_b = [
        {"function": "runtime", "filename": "runtime.js", "lineno": 1},
        {"function": "checkoutB", "filename": "checkout-b.js", "lineno": 84},
    ]

    def exception_properties(frames: list[dict[str, object]]) -> str:
        return json.dumps(
            {
                "$exception_fingerprint": "shared-fingerprint",
                "$exception_list": [
                    {
                        "type": "TypeError",
                        "value": "checkout failed",
                        "stacktrace": {"type": "raw", "frames": frames},
                    }
                ],
            }
        )

    rows = [
        (
            "exception-a",
            1,
            "posthog",
            "$exception",
            "user-a",
            None,
            "2026-07-18 10:00:00",
            "2026-07-18 10:00:00",
            exception_properties(frames_a),
            None,
            None,
            None,
            None,
            None,
            None,
            "phc_project_a",
        ),
        (
            "exception-a-identified",
            1,
            "posthog",
            "$exception",
            "identified-a",
            "person-a",
            "2026-07-18 10:01:00",
            "2026-07-18 10:01:00",
            exception_properties(frames_a),
            None,
            None,
            None,
            None,
            None,
            None,
            "phc_project_a",
        ),
        (
            "exception-b",
            2,
            "posthog",
            "$exception",
            "user-b",
            None,
            "2026-07-18 10:01:00",
            "2026-07-18 10:01:00",
            exception_properties(frames_b),
            json.dumps({"session_id": "context-session-b"}),
            None,
            None,
            None,
            None,
            None,
            "phc_project_b",
        ),
        (
            "trusted-status-a",
            1,
            "hogflare",
            "$error_issue_status",
            "admin",
            None,
            "2026-07-18 10:02:00",
            "2026-07-18 10:02:00",
            json.dumps({"fingerprint": "shared-fingerprint", "status": "resolved"}),
            None,
            None,
            None,
            None,
            None,
            None,
            "phc_project_a",
        ),
        (
            "spoofed-status-b",
            2,
            "posthog",
            "$error_issue_status",
            "attacker",
            None,
            "2026-07-18 10:03:00",
            "2026-07-18 10:03:00",
            json.dumps({"fingerprint": "shared-fingerprint", "status": "ignored"}),
            None,
            None,
            None,
            None,
            None,
            None,
            "phc_project_b",
        ),
    ]
    connection.executemany(
        "insert into events values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )

    event_frames = connection.execute(
        f"select api_key, top_frame_function from ({model_sql('error_events')}) order by api_key"
    ).fetchall()
    assert event_frames == [
        ("phc_project_a", "checkoutA"),
        ("phc_project_a", "checkoutA"),
        ("phc_project_b", "checkoutB"),
    ], event_frames

    resolved_identities = connection.execute(
        f"select api_key, actor_id, identity_id, session_id "
        f"from ({model_sql('error_events')}) order by api_key, actor_id"
    ).fetchall()
    assert resolved_identities == [
        ("phc_project_a", "identified-a", "person-a", None),
        ("phc_project_a", "user-a", "person-a", None),
        ("phc_project_b", "user-b", "user-b", "context-session-b"),
    ], resolved_identities

    issues = connection.execute(
        f"select team_id, api_key, issue_fingerprint, status, event_count, affected_users, affected_sessions "
        f"from ({model_sql('error_issues')}) order by api_key"
    ).fetchall()
    assert issues == [
        (1, "phc_project_a", "shared-fingerprint", "resolved", 2, 1, 0),
        (2, "phc_project_b", "shared-fingerprint", "active", 1, 1, 1),
    ], issues

    print("error model identity, session, isolation, trusted status, and top-frame checks passed")


if __name__ == "__main__":
    main()
