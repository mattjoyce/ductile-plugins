#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""pipeline_watch — Ductile plugin (protocol v2).

The missing supervisor/liveness layer. A scheduled, GENERIC watcher that reads
job outcomes for ALL plugins from the ductile job_log table, detects unhealthy
conditions, records incidents durably, and emits alert events. No plugin or
pipeline names are hardcoded — it applies uniformly to every plugin.

Why this exists: a plugin once crashed and the pipeline silently stopped for
days because the only failure signal was a fire-and-forget Discord webhook that
itself failed. This watcher is the durable, plugin-agnostic backstop: it owns
a queryable incident record and a recovery lifecycle, independent of any one
notification channel.

Detected conditions (both generic):
  failure_streak — a plugin's most recent `streak_threshold` terminal jobs are
                   all failed/timed_out.
  stall          — a plugin that has succeeded before, is still active within
                   the lookback, but has had no success in `stall_minutes`.

Event emitted: ductile.health.alert  (one per NEW incident)
  payload.plugin / payload.kind / payload.detail / payload.since
  dedupe_key — health-alert:<plugin>:<kind>:<first_seen>

Writes only its own table: pipeline_watch_incidents (create-if-not-exists).
Read-only on job_log.

Config keys (all optional):
  db_path           (str) — ductile sqlite db (default ~/.config/ductile/ductile.db)
  streak_threshold  (int) — consecutive failures = incident (default 3)
  stall_minutes     (int) — no-success age that = stall (default 360)
  lookback_minutes  (int) — only consider plugins active within this window (default 1440)
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
from datetime import UTC, datetime, timedelta
from typing import Any, NotRequired, TypedDict

CMD_POLL = "poll"
CMD_HEALTH = "health"
TERMINAL_FAIL = ("failed", "timed_out")
ALERT_EVENT = "ductile.health.alert"
DEFAULT_DB = os.path.expanduser("~/.config/ductile/ductile.db")
DEFAULT_STREAK = 3
DEFAULT_STALL_MIN = 360
DEFAULT_LOOKBACK_MIN = 1440


class LogEntry(TypedDict):
    level: str
    message: str


class ResponseOk(TypedDict):
    status: str
    result: str
    logs: list[LogEntry]
    events: NotRequired[list[dict[str, Any]]]
    state_updates: NotRequired[dict[str, Any]]


class ResponseErr(TypedDict):
    status: str
    error: str
    retry: bool
    logs: list[LogEntry]


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


def ok(
    result: str,
    *,
    logs: list[LogEntry] | None = None,
    events: list[dict[str, Any]] | None = None,
    state_updates: dict[str, Any] | None = None,
) -> ResponseOk:
    resp: ResponseOk = {
        "status": "ok",
        "result": result,
        "logs": logs or [{"level": "info", "message": result}],
    }
    if events:
        resp["events"] = events
    if state_updates is not None:
        resp["state_updates"] = state_updates
    return resp


def err(message: str, *, retry: bool = False, logs: list[LogEntry] | None = None) -> ResponseErr:
    return {
        "status": "error",
        "error": message,
        "retry": retry,
        "logs": logs or [{"level": "error", "message": message}],
    }


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


# ── incident store (the only thing this plugin writes) ──────────────────────────


def _ensure_incident_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_watch_incidents (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            plugin      TEXT NOT NULL,
            kind        TEXT NOT NULL,
            detail      TEXT,
            first_seen  TEXT NOT NULL,
            last_seen   TEXT NOT NULL,
            resolved_at TEXT,
            alerted     INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.commit()


# ── detection (read-only on job_log) ────────────────────────────────────────────


def _active_plugins(conn: sqlite3.Connection, since_iso: str) -> list[str]:
    rows = conn.execute(
        "SELECT DISTINCT plugin FROM job_log WHERE completed_at >= ?",
        (since_iso,),
    ).fetchall()
    return [r[0] for r in rows]


def _recent_statuses(conn: sqlite3.Connection, plugin: str, limit: int) -> list[str]:
    rows = conn.execute(
        "SELECT status FROM job_log WHERE plugin = ? ORDER BY completed_at DESC LIMIT ?",
        (plugin, limit),
    ).fetchall()
    return [r[0] for r in rows]


def _latest_error(conn: sqlite3.Connection, plugin: str) -> str:
    row = conn.execute(
        "SELECT last_error FROM job_log WHERE plugin = ? AND status IN ('failed','timed_out') "
        "ORDER BY completed_at DESC LIMIT 1",
        (plugin,),
    ).fetchone()
    return (row[0] if row and row[0] else "").strip()[:300]


def _last_success(conn: sqlite3.Connection, plugin: str) -> datetime | None:
    row = conn.execute(
        "SELECT MAX(completed_at) FROM job_log WHERE plugin = ? AND status = 'succeeded'",
        (plugin,),
    ).fetchone()
    return _parse_ts(row[0] if row else None)


def detect(conn: sqlite3.Connection, cfg: dict[str, Any]) -> dict[tuple[str, str], str]:
    """Return {(plugin, kind): detail} for every currently-unhealthy condition."""
    streak_n = int(cfg.get("streak_threshold") or DEFAULT_STREAK)
    stall_min = int(cfg.get("stall_minutes") or DEFAULT_STALL_MIN)
    lookback_min = int(cfg.get("lookback_minutes") or DEFAULT_LOOKBACK_MIN)

    now = datetime.now(UTC)
    since_iso = (now - timedelta(minutes=lookback_min)).isoformat()
    found: dict[tuple[str, str], str] = {}

    for plugin in _active_plugins(conn, since_iso):
        statuses = _recent_statuses(conn, plugin, streak_n)
        if len(statuses) >= streak_n and all(s in TERMINAL_FAIL for s in statuses):
            detail = f"{streak_n} consecutive failures; latest: {_latest_error(conn, plugin) or 'n/a'}"
            found[(plugin, "failure_streak")] = detail

        last_ok = _last_success(conn, plugin)
        if last_ok is not None:
            age_min = (now - last_ok).total_seconds() / 60.0
            if age_min > stall_min:
                found[(plugin, "stall")] = (
                    f"no success in {int(age_min)} min (last ok {last_ok.isoformat()})"
                )

    return found


# ── incident lifecycle ───────────────────────────────────────────────────────────


def reconcile(
    conn: sqlite3.Connection, detected: dict[tuple[str, str], str]
) -> tuple[list[dict[str, Any]], int, int]:
    """Open new incidents (→ alert events), refresh ongoing, resolve recovered."""
    now = now_iso()
    open_rows = conn.execute(
        "SELECT id, plugin, kind, first_seen FROM pipeline_watch_incidents WHERE resolved_at IS NULL"
    ).fetchall()
    open_map = {(r[1], r[2]): (r[0], r[3]) for r in open_rows}

    events: list[dict[str, Any]] = []
    new_count = 0

    for (plugin, kind), detail in detected.items():
        if (plugin, kind) in open_map:
            conn.execute(
                "UPDATE pipeline_watch_incidents SET last_seen = ?, detail = ? WHERE id = ?",
                (now, detail, open_map[(plugin, kind)][0]),
            )
            continue
        conn.execute(
            "INSERT INTO pipeline_watch_incidents "
            "(plugin, kind, detail, first_seen, last_seen, alerted) VALUES (?,?,?,?,?,1)",
            (plugin, kind, detail, now, now),
        )
        new_count += 1
        events.append(
            {
                "type": ALERT_EVENT,
                "payload": {
                    "plugin": plugin,
                    "kind": kind,
                    "detail": detail,
                    "since": now,
                    # discord_notify (and other sinks) read `message` from the
                    # payload directly — ship a ready-to-send line.
                    "message": f"🩺 ductile health alert\n**{plugin}** — {kind}\n{detail}",
                },
                "dedupe_key": f"health-alert:{plugin}:{kind}:{now}",
            }
        )

    resolved_count = 0
    for (plugin, kind), (inc_id, _first) in open_map.items():
        if (plugin, kind) not in detected:
            conn.execute(
                "UPDATE pipeline_watch_incidents SET resolved_at = ? WHERE id = ?", (now, inc_id)
            )
            resolved_count += 1

    conn.commit()
    return events, new_count, resolved_count


# ── commands ───────────────────────────────────────────────────────────────────


def _connect(cfg: dict[str, Any]) -> sqlite3.Connection:
    db_path = str(cfg.get("db_path") or DEFAULT_DB)
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"ductile db not found: {db_path}")
    return sqlite3.connect(db_path, timeout=10)


def cmd_poll(config: dict[str, Any], state: dict[str, Any]) -> ResponseOk | ResponseErr:
    try:
        conn = _connect(config)
    except Exception as exc:  # noqa: BLE001 — never crash; report as error
        return err(f"cannot open ductile db: {exc}", retry=True)

    try:
        _ensure_incident_table(conn)
        detected = detect(conn, config)
        events, new_count, resolved_count = reconcile(conn, detected)
    except Exception as exc:  # noqa: BLE001 — never crash the watcher
        conn.close()
        return err(f"pipeline_watch poll failed: {exc}", retry=True)
    finally:
        if conn:
            conn.close()

    open_now = len(detected)
    summary = (
        f"checked job_log; {open_now} open incident(s) "
        f"({new_count} new, {resolved_count} resolved)"
    )
    logs: list[LogEntry] = [{"level": "info", "message": summary}]
    for (plugin, kind), detail in detected.items():
        logs.append({"level": "warn", "message": f"{plugin}/{kind}: {detail}"})

    return ok(
        summary,
        logs=logs,
        events=events or None,
        state_updates={"last_poll_at": now_iso()},
    )


def cmd_health(config: dict[str, Any]) -> ResponseOk | ResponseErr:
    streak_n = int(config.get("streak_threshold") or DEFAULT_STREAK)
    stall_min = int(config.get("stall_minutes") or DEFAULT_STALL_MIN)
    try:
        conn = _connect(config)
        conn.execute("SELECT 1 FROM job_log LIMIT 1").fetchone()
        conn.close()
    except Exception as exc:  # noqa: BLE001
        return err(f"db not queryable: {exc}", retry=True)
    msg = (
        f"pipeline_watch healthy — job_log queryable; "
        f"streak_threshold={streak_n}, stall_minutes={stall_min}"
    )
    return ok(msg, logs=[{"level": "info", "message": msg}])


# ── entrypoint ─────────────────────────────────────────────────────────────────


def main() -> None:
    try:
        req = json.load(sys.stdin)
    except json.JSONDecodeError as exc:
        json.dump(err(f"Invalid JSON input: {exc}"), sys.stdout)
        sys.stdout.write("\n")
        sys.exit(1)

    if not isinstance(req, dict):
        json.dump(err("request body must be a JSON object"), sys.stdout)
        sys.stdout.write("\n")
        sys.exit(1)

    command = req.get("command", "")
    config = req.get("config", {})
    if not isinstance(config, dict):
        config = {}
    state = req.get("state", {})
    if not isinstance(state, dict):
        state = {}

    out: ResponseOk | ResponseErr
    if command == CMD_POLL:
        out = cmd_poll(config, state)
    elif command == CMD_HEALTH:
        out = cmd_health(config)
    else:
        out = err(f"Unknown command: {command!r}")

    json.dump(out, sys.stdout)
    sys.stdout.write("\n")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
