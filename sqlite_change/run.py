#!/usr/bin/env python3
"""sqlite_change plugin for Ductile (protocol v2).

Monitors a SQLite database by running a configured SQL query. If the result
meets a threshold condition, emits a configurable event. State is managed
per-instance by Ductile.
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


COMPARISON_OPS = {">", ">=", "<", "<=", "=="}
ALL_OPS = COMPARISON_OPS | {"changed", "any_rows"}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def error_response(message: str, retry: bool = False, logs: Optional[List[Dict]] = None) -> Dict[str, Any]:
    return {
        "status": "error",
        "error": message,
        "retry": retry,
        "events": [],
        "logs": logs or [{"level": "error", "message": message}],
    }


def ok_response(
    result: str,
    events: Optional[List[Dict]] = None,
    state_updates: Optional[Dict] = None,
    logs: Optional[List[Dict]] = None,
) -> Dict[str, Any]:
    resp: Dict[str, Any] = {
        "status": "ok",
        "result": result,
        "events": events or [],
        "logs": logs or [],
    }
    if state_updates:
        resp["state_updates"] = state_updates
    return resp


def render_message(template: str, fields: Dict[str, Any]) -> str:
    """Render a simple {key} template from fields dict. Missing keys render as empty string."""
    import re
    def replacer(m: re.Match) -> str:
        val = fields.get(m.group(1))
        return "" if val is None else str(val)
    return re.sub(r"\{(\w+)\}", replacer, template)


def validate_config(config: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    for key in ("db_path", "query", "event_type"):
        if not config.get(key):
            errors.append(f"config.{key} is required")

    threshold_op = config.get("threshold_op", "changed")
    if threshold_op not in ALL_OPS:
        errors.append(f"config.threshold_op must be one of: {', '.join(sorted(ALL_OPS))}")
    elif threshold_op in COMPARISON_OPS and config.get("threshold_value") is None:
        errors.append(f"config.threshold_value is required when threshold_op is '{threshold_op}'")

    return errors


def run_query(db_path: str, query: str) -> Tuple[Optional[str], bool]:
    """Run query in read-only mode. Returns (scalar_result_str, had_rows)."""
    uri = f"file:{db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    try:
        cur = conn.execute(query)
        row = cur.fetchone()
        if row is None:
            return None, False
        val = row[0]
        return (str(val) if val is not None else None), True
    finally:
        conn.close()


def evaluate_threshold(
    scalar: Optional[str],
    had_rows: bool,
    threshold_op: str,
    threshold_value: Optional[float],
    last_result: Optional[str],
) -> bool:
    if threshold_op == "any_rows":
        return had_rows

    if threshold_op == "changed":
        return scalar != last_result

    # Numeric comparison ops
    numeric = float(scalar) if scalar is not None else 0.0
    if threshold_op == ">":
        return numeric > threshold_value  # type: ignore[operator]
    if threshold_op == ">=":
        return numeric >= threshold_value  # type: ignore[operator]
    if threshold_op == "<":
        return numeric < threshold_value  # type: ignore[operator]
    if threshold_op == "<=":
        return numeric <= threshold_value  # type: ignore[operator]
    if threshold_op == "==":
        return numeric == threshold_value  # type: ignore[operator]

    raise ValueError(f"unsupported threshold_op: {threshold_op}")


def snapshot_state(
    *,
    last_result: Optional[str],
    last_checked_at: str,
    last_triggered_at: Optional[str],
) -> Dict[str, Any]:
    """Return the full compatibility snapshot for poll-owned durable state."""
    return {
        "last_result": last_result,
        "last_checked_at": last_checked_at,
        "last_triggered_at": last_triggered_at,
    }


def poll_command(config: Dict[str, Any], state: Dict[str, Any], instance: str) -> Dict[str, Any]:
    errors = validate_config(config)
    if errors:
        return error_response("; ".join(errors), retry=False)

    db_path = config["db_path"]
    query = config["query"]
    event_type = config["event_type"]
    threshold_op = config.get("threshold_op", "changed")
    threshold_value = config.get("threshold_value")
    if threshold_value is not None:
        threshold_value = float(threshold_value)

    last_result: Optional[str] = state.get("last_result")

    if not os.path.exists(db_path):
        return error_response(f"db_path not found: {db_path}", retry=True)

    try:
        scalar, had_rows = run_query(db_path, query)
    except Exception as exc:
        return error_response(f"query failed: {exc}", retry=True)

    timestamp = now_iso()
    triggered = evaluate_threshold(scalar, had_rows, threshold_op, threshold_value, last_result)

    prior_triggered_at = state.get("last_triggered_at")
    state_updates = snapshot_state(
        last_result=scalar,
        last_checked_at=timestamp,
        last_triggered_at=timestamp if triggered else prior_triggered_at,
    )

    if triggered:
        payload_fields = {
            "source": "sqlite_change",
            "instance": instance,
            "db_path": db_path,
            "query": query,
            "result": scalar,
            "previous_result": last_result,
            "threshold_op": threshold_op,
            "threshold_value": threshold_value,
            "detected_at": timestamp,
        }
        msg_template = config.get(
            "message_template",
            "sqlite_change [{instance}]: {threshold_op} triggered — {result} (was: {previous_result})",
        )
        payload_fields["message"] = render_message(msg_template, payload_fields)
        event = {
            "type": event_type,
            "payload": payload_fields,
        }
        return ok_response(
            f"threshold met ({threshold_op}): {scalar}",
            events=[event],
            state_updates=state_updates,
            logs=[{"level": "info", "message": f"threshold met, emitting {event_type}"}],
        )

    return ok_response(
        f"threshold not met ({threshold_op}): {scalar}",
        state_updates=state_updates,
        logs=[{"level": "debug", "message": f"threshold not met, result={scalar}"}],
    )


def health_command(config: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Any]:
    errors = validate_config(config)
    if errors:
        return error_response("; ".join(errors), retry=False)

    db_path = config["db_path"]
    db_exists = os.path.exists(db_path)

    scalar: Optional[str] = None
    query_error: Optional[str] = None

    if db_exists:
        try:
            scalar, _ = run_query(db_path, config["query"])
        except Exception as exc:
            query_error = str(exc)

    health_info = {
        "db_exists": db_exists,
        "current_result": scalar,
        "query_error": query_error,
        "state": state,
    }

    return ok_response(
        "ok" if db_exists and query_error is None else "degraded",
        logs=[{"level": "info", "message": json.dumps(health_info)}],
    )


def handle_request(request: Dict[str, Any]) -> Dict[str, Any]:
    command = request.get("command", "")
    config = request.get("config") if isinstance(request.get("config"), dict) else {}
    state = request.get("state") if isinstance(request.get("state"), dict) else {}
    instance = request.get("instance", "")

    if command == "poll":
        return poll_command(config, state, instance)
    if command == "health":
        return health_command(config, state)

    return error_response(f"unknown command: {command}")


def main() -> None:
    try:
        request = json.load(sys.stdin)
    except json.JSONDecodeError as exc:
        response = error_response(f"Invalid JSON input: {exc}", retry=False)
        json.dump(response, sys.stdout)
        sys.exit(1)
    response = handle_request(request)
    json.dump(response, sys.stdout)


if __name__ == "__main__":
    main()
