#!/usr/bin/env python3
"""birdnet_firstday plugin for Ductile (protocol v2).

Polls the birdnet-go SQLite detections DB. On each poll, emits one event per
species whose earliest detection today (local calendar day) has an id greater
than the persisted watermark. Common names are resolved via the birdnet-go
range/species API (same host) and kept in the poll snapshot used for the next
run's enrichment behavior.
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


DEFAULT_EVENT_TYPE = "birdnet.firstday_species"
DEFAULT_CACHE_TTL_SECONDS = 3600
DEFAULT_HTTP_TIMEOUT_SECONDS = 5


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


def validate_config(config: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    if not config.get("db_path"):
        errors.append("config.db_path is required")
    return errors


def fetch_species_map(url: str, timeout: float) -> Dict[str, str]:
    """Fetch {scientific_name: common_name} from birdnet-go range/species/list API.

    Raises on HTTP or JSON error — caller decides whether to fallback.
    """
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    species = data.get("species") or []
    return {
        item["scientificName"]: item["commonName"]
        for item in species
        if item.get("scientificName") and item.get("commonName")
    }


def load_species_cache(
    state: Dict[str, Any],
    url: Optional[str],
    ttl_seconds: int,
    timeout: float,
) -> Tuple[Dict[str, str], Optional[str], Optional[str]]:
    """Return (species_map, fetched_at_iso or None, warning or None).

    If url is unset, returns an empty map (scientific-name-only mode).
    If url is set and cache is fresh, returns cached map.
    If url is set and cache is stale, tries to refresh; on failure, falls back
    to the stale cache (or empty) and returns a warning.
    """
    cached: Dict[str, str] = state.get("species_cache") or {}
    fetched_at_iso: Optional[str] = state.get("species_cache_fetched_at")

    if not url:
        return {}, None, None

    if fetched_at_iso and cached:
        try:
            fetched_at = datetime.fromisoformat(fetched_at_iso)
            age = (datetime.now(timezone.utc) - fetched_at).total_seconds()
            if age < ttl_seconds:
                return cached, fetched_at_iso, None
        except ValueError:
            pass

    try:
        fresh = fetch_species_map(url, timeout)
        if fresh:
            return fresh, now_iso(), None
        return cached, fetched_at_iso, "species API returned empty list; using stale cache"
    except Exception as exc:
        return cached, fetched_at_iso, f"species API fetch failed: {exc}"


def query_first_of_day(
    db_path: str,
    watermark: int,
) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """Return (new_species_rows, today_max_id).

    Each row has: scientific_name, first_id, first_ts (unix int), peak_conf.
    today_max_id is the MAX(id) over all of today's detections (for advancing
    the watermark), or None if today has no detections.
    """
    uri = f"file:{db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    try:
        main_sql = """
            SELECT l.scientific_name,
                   MIN(d.id)           AS first_id,
                   MIN(d.detected_at)  AS first_ts,
                   MAX(d.confidence)   AS peak_conf
              FROM detections d
              JOIN labels l ON l.id = d.label_id
             WHERE DATE(d.detected_at, 'unixepoch', 'localtime')
                 = DATE('now', 'localtime')
             GROUP BY l.scientific_name
            HAVING MIN(d.id) > ?
             ORDER BY first_id
        """
        cur = conn.execute(main_sql, (watermark,))
        rows = [
            {
                "scientific_name": r[0],
                "first_id": int(r[1]),
                "first_ts": int(r[2]),
                "peak_conf": float(r[3]) if r[3] is not None else None,
            }
            for r in cur.fetchall()
        ]

        max_sql = """
            SELECT MAX(id) FROM detections
             WHERE DATE(detected_at, 'unixepoch', 'localtime')
                 = DATE('now', 'localtime')
        """
        max_row = conn.execute(max_sql).fetchone()
        today_max_id = int(max_row[0]) if max_row and max_row[0] is not None else None
        return rows, today_max_id
    finally:
        conn.close()


def format_local_time(unix_ts: int) -> str:
    """Format a unix timestamp as local H:M (system TZ, expected Australia/Sydney)."""
    return datetime.fromtimestamp(unix_ts).strftime("%H:%M")


def build_event(
    row: Dict[str, Any],
    species_map: Dict[str, str],
    event_type: str,
    instance: str,
) -> Dict[str, Any]:
    sci = row["scientific_name"]
    common = species_map.get(sci, "")
    peak_conf = row["peak_conf"]
    payload = {
        "source": "birdnet_firstday",
        "instance": instance,
        "scientific_name": sci,
        "common_name": common,
        "first_id": row["first_id"],
        "first_ts": row["first_ts"],
        "first_time": format_local_time(row["first_ts"]),
        "peak_conf": peak_conf,
        "peak_conf_pct": int(round(peak_conf * 100)) if peak_conf is not None else None,
        "detected_at": now_iso(),
    }
    display = common or sci
    payload["message"] = (
        f"🐦 {display} first heard today at {payload['first_time']}"
        if common
        else f"🐦 {sci} — first heard today at {payload['first_time']}"
    )
    return {"type": event_type, "payload": payload}


def snapshot_state(
    *,
    watermark: int,
    last_polled_at: str,
    species_url: Optional[str],
    species_map: Dict[str, str],
    species_fetched_at: Optional[str],
) -> Dict[str, Any]:
    """Return the full compatibility snapshot for poll-owned state."""
    state_updates: Dict[str, Any] = {
        "watermark": watermark,
        "last_polled_at": last_polled_at,
    }
    if species_url:
        state_updates["species_cache"] = species_map
        if species_fetched_at:
            state_updates["species_cache_fetched_at"] = species_fetched_at
    return state_updates


def poll_command(config: Dict[str, Any], state: Dict[str, Any], instance: str) -> Dict[str, Any]:
    errors = validate_config(config)
    if errors:
        return error_response("; ".join(errors), retry=False)

    db_path = config["db_path"]
    species_url = config.get("species_url")
    event_type = config.get("event_type", DEFAULT_EVENT_TYPE)
    cache_ttl = int(config.get("cache_ttl_seconds", DEFAULT_CACHE_TTL_SECONDS))
    http_timeout = float(config.get("http_timeout_seconds", DEFAULT_HTTP_TIMEOUT_SECONDS))

    if not os.path.exists(db_path):
        return error_response(f"db_path not found: {db_path}", retry=True)

    watermark = int(state.get("watermark", 0) or 0)

    species_map, species_fetched_at, species_warning = load_species_cache(
        state, species_url, cache_ttl, http_timeout
    )

    try:
        rows, today_max_id = query_first_of_day(db_path, watermark)
    except Exception as exc:
        return error_response(f"db query failed: {exc}", retry=True)

    events = [build_event(r, species_map, event_type, instance) for r in rows]

    new_watermark = max(watermark, today_max_id) if today_max_id is not None else watermark
    state_updates = snapshot_state(
        watermark=new_watermark,
        last_polled_at=now_iso(),
        species_url=species_url,
        species_map=species_map,
        species_fetched_at=species_fetched_at,
    )

    logs: List[Dict[str, str]] = []
    if species_warning:
        logs.append({"level": "warn", "message": species_warning})
    if events:
        logs.append({
            "level": "info",
            "message": f"emitting {len(events)} first-of-day events (watermark {watermark} -> {new_watermark})",
        })

    return ok_response(
        f"{len(events)} new first-of-day species (watermark={new_watermark})",
        events=events,
        state_updates=state_updates,
        logs=logs,
    )


def health_command(config: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Any]:
    errors = validate_config(config)
    if errors:
        return error_response("; ".join(errors), retry=False)

    db_path = config["db_path"]
    db_exists = os.path.exists(db_path)
    today_count: Optional[int] = None
    query_error: Optional[str] = None

    if db_exists:
        try:
            uri = f"file:{db_path}?mode=ro"
            conn = sqlite3.connect(uri, uri=True)
            try:
                sql = """
                    SELECT COUNT(*) FROM detections
                     WHERE DATE(detected_at, 'unixepoch', 'localtime')
                         = DATE('now', 'localtime')
                """
                today_count = int(conn.execute(sql).fetchone()[0])
            finally:
                conn.close()
        except Exception as exc:
            query_error = str(exc)

    info = {
        "db_exists": db_exists,
        "today_detection_count": today_count,
        "query_error": query_error,
        "watermark": int(state.get("watermark", 0) or 0),
        "species_cache_size": len(state.get("species_cache") or {}),
        "species_cache_fetched_at": state.get("species_cache_fetched_at"),
    }
    return ok_response(
        "ok" if db_exists and query_error is None else "degraded",
        logs=[{"level": "info", "message": json.dumps(info)}],
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
        json.dump(error_response(f"Invalid JSON input: {exc}", retry=False), sys.stdout)
        sys.exit(1)
    json.dump(handle_request(request), sys.stdout)


if __name__ == "__main__":
    main()
