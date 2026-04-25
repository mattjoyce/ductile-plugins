#!/usr/bin/env python3
"""health_data_summary — ductile plugin (protocol v2).

Consumes health.new_data events from upstream collector plugins (garmin,
withings, or any other source that emits the same payload shape). For each
dirty day in the event payload, joins data from garmin.db and withings.db
and upserts a unified row into daily_health_summary in summary.db.

Commands:
  summarize : Process dirty_periods from the event payload, write to summary.db
  query     : Read daily summaries for a date range
  health    : Report plugin health and summary-DB stats (no state_updates)

State snapshot (recorded as health_data_summary.snapshot fact via fact_outputs):
  latest_summarized_day : max(day) FROM daily_health_summary
  summary_row_count     : count(*) FROM daily_health_summary

Both fields are observed durable state (computed from summary.db at the end
of every successful summarize), not action bookkeeping. Action provenance
(what this invocation wrote, when, from which source) is captured in
job_log automatically.
"""

import json
import sqlite3
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


SUMMARY_SCHEMA = """
CREATE TABLE IF NOT EXISTS daily_health_summary (
    day DATE PRIMARY KEY,

    -- Garmin: daily_summary
    steps INTEGER,
    calories_active INTEGER,
    calories_total INTEGER,
    distance_km REAL,
    floors_up REAL,
    stress_avg INTEGER,
    moderate_activity_min INTEGER,
    vigorous_activity_min INTEGER,
    spo2_avg REAL,
    spo2_min REAL,
    rr_avg REAL,
    body_battery_max INTEGER,
    body_battery_min INTEGER,
    hr_min INTEGER,
    hr_max INTEGER,

    -- Garmin: sleep
    sleep_total_min INTEGER,
    sleep_deep_min INTEGER,
    sleep_light_min INTEGER,
    sleep_rem_min INTEGER,
    sleep_score INTEGER,
    sleep_qualifier TEXT,

    -- Garmin: resting_hr (dedicated table)
    resting_hr INTEGER,

    -- Withings: body composition (type 1=weight, 6=fat_ratio, 5=fat_free_mass,
    --   8=fat_mass, 76=muscle_mass, 77=hydration, 88=bone_mass, 226=bmr)
    weight_kg REAL,
    fat_ratio REAL,
    fat_mass_kg REAL,
    fat_free_mass_kg REAL,
    muscle_mass_kg REAL,
    hydration_kg REAL,
    bone_mass_kg REAL,
    bmr_kcal REAL,

    -- Withings: BP + HR
    systolic_bp INTEGER,
    diastolic_bp INTEGER,
    heart_rate_bpm INTEGER,

    -- Meta
    garmin_updated_at TIMESTAMP,
    withings_updated_at TIMESTAMP,
    summarized_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

# Withings measure type IDs
WITHINGS_TYPES = {
    1: "weight_kg",
    5: "fat_free_mass_kg",
    6: "fat_ratio",
    8: "fat_mass_kg",
    9: "diastolic_bp",
    10: "systolic_bp",
    11: "heart_rate_bpm",
    76: "muscle_mass_kg",
    77: "hydration_kg",
    88: "bone_mass_kg",
    226: "bmr_kcal",
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def log(level: str, message: str) -> Dict[str, str]:
    return {"level": level, "message": message}


def ok_response(
    *,
    result: str,
    events: Optional[List[Dict[str, Any]]] = None,
    state_updates: Optional[Dict[str, Any]] = None,
    logs: Optional[List[Dict[str, str]]] = None,
) -> Dict[str, Any]:
    response: Dict[str, Any] = {"status": "ok", "result": result}
    if events:
        response["events"] = events
    if state_updates is not None:
        response["state_updates"] = state_updates
    response["logs"] = logs or []
    return response


def error_response(message: str, *, retry: bool = True) -> Dict[str, Any]:
    return {
        "status": "error",
        "error": message,
        "retry": retry,
        "logs": [log("error", message)],
    }


def snapshot_state(*, latest_summarized_day: Optional[str], summary_row_count: int) -> Dict[str, Any]:
    """Pure constructor for the full compatibility-view snapshot.

    Every field is required at every call site — the helper never inherits
    silently. Both fields are observations of summary.db, not action trace.
    Presence-stable: same keys every invocation.
    """
    return {
        "latest_summarized_day": latest_summarized_day,
        "summary_row_count": summary_row_count,
    }


def observe_summary_state(summary_conn: sqlite3.Connection) -> Dict[str, Any]:
    """Observe durable state of summary.db and return the snapshot."""
    row = summary_conn.execute(
        "SELECT MAX(day) AS latest_day, COUNT(*) AS row_count FROM daily_health_summary"
    ).fetchone()
    return snapshot_state(
        latest_summarized_day=row["latest_day"] if row else None,
        summary_row_count=int(row["row_count"]) if row else 0,
    )


def time_to_minutes(time_str: Optional[str]) -> Optional[int]:
    """Convert HH:MM:SS or HH:MM:SS.ffffff to total minutes."""
    if not time_str:
        return None
    try:
        parts = time_str.split(".")[0].split(":")
        h, m, s = int(parts[0]), int(parts[1]), int(parts[2])
        return h * 60 + m + (1 if s >= 30 else 0)
    except (ValueError, IndexError):
        return None


def open_ro(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def open_rw(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_summary_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SUMMARY_SCHEMA)
    conn.commit()


def fetch_garmin_day(garmin_conn: sqlite3.Connection, day: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {}

    row = garmin_conn.execute(
        "SELECT * FROM daily_summary WHERE day = ?", (day,)
    ).fetchone()
    if row:
        result.update({
            "steps": row["steps"],
            "calories_active": row["calories_active"],
            "calories_total": row["calories_total"],
            "distance_km": round(float(row["distance"]), 3) if row["distance"] else None,
            "floors_up": row["floors_up"],
            "stress_avg": row["stress_avg"],
            "moderate_activity_min": time_to_minutes(row["moderate_activity_time"]),
            "vigorous_activity_min": time_to_minutes(row["vigorous_activity_time"]),
            "spo2_avg": row["spo2_avg"],
            "spo2_min": row["spo2_min"],
            "rr_avg": row["rr_waking_avg"],
            "body_battery_max": row["bb_max"],
            "body_battery_min": row["bb_min"],
            "hr_min": row["hr_min"],
            "hr_max": row["hr_max"],
        })

    sleep_row = garmin_conn.execute(
        "SELECT * FROM sleep WHERE day = ?", (day,)
    ).fetchone()
    if sleep_row:
        result.update({
            "sleep_total_min": time_to_minutes(sleep_row["total_sleep"]),
            "sleep_deep_min": time_to_minutes(sleep_row["deep_sleep"]),
            "sleep_light_min": time_to_minutes(sleep_row["light_sleep"]),
            "sleep_rem_min": time_to_minutes(sleep_row["rem_sleep"]),
            "sleep_score": sleep_row["score"],
            "sleep_qualifier": sleep_row["qualifier"],
        })

    hr_row = garmin_conn.execute(
        "SELECT resting_heart_rate FROM resting_hr WHERE day = ?", (day,)
    ).fetchone()
    if hr_row and hr_row["resting_heart_rate"]:
        result["resting_hr"] = int(hr_row["resting_heart_rate"])

    return result


def fetch_withings_day(withings_conn: sqlite3.Connection, day: str) -> Dict[str, Any]:
    """Fetch the most recent measurement of each type for the given day."""
    rows = withings_conn.execute(
        """
        SELECT type, value
        FROM measurements
        WHERE DATE(date) = ?
        GROUP BY type
        HAVING date = MAX(date)
        """,
        (day,),
    ).fetchall()

    result: Dict[str, Any] = {}
    for row in rows:
        field = WITHINGS_TYPES.get(int(row["type"]))
        if field:
            result[field] = float(row["value"])

    for int_field in ("systolic_bp", "diastolic_bp", "heart_rate_bpm"):
        if int_field in result:
            result[int_field] = int(round(result[int_field]))

    return result


def upsert_summary_day(
    summary_conn: sqlite3.Connection,
    day: str,
    garmin: Dict[str, Any],
    withings: Dict[str, Any],
    now_ts: str,
) -> None:
    garmin_updated = now_ts if garmin else None
    withings_updated = now_ts if withings else None

    summary_conn.execute(
        """
        INSERT INTO daily_health_summary (
            day,
            steps, calories_active, calories_total, distance_km, floors_up,
            stress_avg, moderate_activity_min, vigorous_activity_min,
            spo2_avg, spo2_min, rr_avg, body_battery_max, body_battery_min,
            hr_min, hr_max,
            sleep_total_min, sleep_deep_min, sleep_light_min, sleep_rem_min,
            sleep_score, sleep_qualifier,
            resting_hr,
            weight_kg, fat_ratio, fat_mass_kg, fat_free_mass_kg,
            muscle_mass_kg, hydration_kg, bone_mass_kg, bmr_kcal,
            systolic_bp, diastolic_bp, heart_rate_bpm,
            garmin_updated_at, withings_updated_at, summarized_at
        ) VALUES (
            :day,
            :steps, :calories_active, :calories_total, :distance_km, :floors_up,
            :stress_avg, :moderate_activity_min, :vigorous_activity_min,
            :spo2_avg, :spo2_min, :rr_avg, :body_battery_max, :body_battery_min,
            :hr_min, :hr_max,
            :sleep_total_min, :sleep_deep_min, :sleep_light_min, :sleep_rem_min,
            :sleep_score, :sleep_qualifier,
            :resting_hr,
            :weight_kg, :fat_ratio, :fat_mass_kg, :fat_free_mass_kg,
            :muscle_mass_kg, :hydration_kg, :bone_mass_kg, :bmr_kcal,
            :systolic_bp, :diastolic_bp, :heart_rate_bpm,
            :garmin_updated_at, :withings_updated_at, :summarized_at
        )
        ON CONFLICT(day) DO UPDATE SET
            steps = COALESCE(excluded.steps, steps),
            calories_active = COALESCE(excluded.calories_active, calories_active),
            calories_total = COALESCE(excluded.calories_total, calories_total),
            distance_km = COALESCE(excluded.distance_km, distance_km),
            floors_up = COALESCE(excluded.floors_up, floors_up),
            stress_avg = COALESCE(excluded.stress_avg, stress_avg),
            moderate_activity_min = COALESCE(excluded.moderate_activity_min, moderate_activity_min),
            vigorous_activity_min = COALESCE(excluded.vigorous_activity_min, vigorous_activity_min),
            spo2_avg = COALESCE(excluded.spo2_avg, spo2_avg),
            spo2_min = COALESCE(excluded.spo2_min, spo2_min),
            rr_avg = COALESCE(excluded.rr_avg, rr_avg),
            body_battery_max = COALESCE(excluded.body_battery_max, body_battery_max),
            body_battery_min = COALESCE(excluded.body_battery_min, body_battery_min),
            hr_min = COALESCE(excluded.hr_min, hr_min),
            hr_max = COALESCE(excluded.hr_max, hr_max),
            sleep_total_min = COALESCE(excluded.sleep_total_min, sleep_total_min),
            sleep_deep_min = COALESCE(excluded.sleep_deep_min, sleep_deep_min),
            sleep_light_min = COALESCE(excluded.sleep_light_min, sleep_light_min),
            sleep_rem_min = COALESCE(excluded.sleep_rem_min, sleep_rem_min),
            sleep_score = COALESCE(excluded.sleep_score, sleep_score),
            sleep_qualifier = COALESCE(excluded.sleep_qualifier, sleep_qualifier),
            resting_hr = COALESCE(excluded.resting_hr, resting_hr),
            weight_kg = COALESCE(excluded.weight_kg, weight_kg),
            fat_ratio = COALESCE(excluded.fat_ratio, fat_ratio),
            fat_mass_kg = COALESCE(excluded.fat_mass_kg, fat_mass_kg),
            fat_free_mass_kg = COALESCE(excluded.fat_free_mass_kg, fat_free_mass_kg),
            muscle_mass_kg = COALESCE(excluded.muscle_mass_kg, muscle_mass_kg),
            hydration_kg = COALESCE(excluded.hydration_kg, hydration_kg),
            bone_mass_kg = COALESCE(excluded.bone_mass_kg, bone_mass_kg),
            bmr_kcal = COALESCE(excluded.bmr_kcal, bmr_kcal),
            systolic_bp = COALESCE(excluded.systolic_bp, systolic_bp),
            diastolic_bp = COALESCE(excluded.diastolic_bp, diastolic_bp),
            heart_rate_bpm = COALESCE(excluded.heart_rate_bpm, heart_rate_bpm),
            garmin_updated_at = COALESCE(excluded.garmin_updated_at, garmin_updated_at),
            withings_updated_at = COALESCE(excluded.withings_updated_at, withings_updated_at),
            summarized_at = excluded.summarized_at
        """,
        {
            "day": day,
            **{k: garmin.get(k) for k in [
                "steps", "calories_active", "calories_total", "distance_km", "floors_up",
                "stress_avg", "moderate_activity_min", "vigorous_activity_min",
                "spo2_avg", "spo2_min", "rr_avg", "body_battery_max", "body_battery_min",
                "hr_min", "hr_max",
                "sleep_total_min", "sleep_deep_min", "sleep_light_min", "sleep_rem_min",
                "sleep_score", "sleep_qualifier", "resting_hr",
            ]},
            **{k: withings.get(k) for k in [
                "weight_kg", "fat_ratio", "fat_mass_kg", "fat_free_mass_kg",
                "muscle_mass_kg", "hydration_kg", "bone_mass_kg", "bmr_kcal",
                "systolic_bp", "diastolic_bp", "heart_rate_bpm",
            ]},
            "garmin_updated_at": garmin_updated,
            "withings_updated_at": withings_updated,
            "summarized_at": now_ts,
        },
    )


def extract_event_payload(request: Dict[str, Any]) -> Dict[str, Any]:
    event = request.get("event")
    if isinstance(event, dict):
        payload = event.get("payload")
        if isinstance(payload, dict):
            return payload
    config = request.get("config")
    if isinstance(config, dict):
        return config
    return {}


def summarize_command(request: Dict[str, Any]) -> Dict[str, Any]:
    config = request.get("config") or {}

    garmin_db_path = str(config.get("garmin_db_path", "")).strip()
    withings_db_path = str(config.get("withings_db_path", "")).strip()
    summary_db_path = str(config.get("summary_db_path", "")).strip()

    missing = [k for k, v in [
        ("garmin_db_path", garmin_db_path),
        ("withings_db_path", withings_db_path),
        ("summary_db_path", summary_db_path),
    ] if not v]
    if missing:
        return error_response(f"Missing required config key(s): {', '.join(missing)}", retry=False)

    payload = extract_event_payload(request)
    dirty_periods = payload.get("dirty_periods")
    source = payload.get("source", "unknown")

    if not dirty_periods or not isinstance(dirty_periods, list):
        return error_response("Event payload missing dirty_periods list", retry=False)

    garmin_conn: Optional[sqlite3.Connection] = None
    withings_conn: Optional[sqlite3.Connection] = None
    summary_conn: Optional[sqlite3.Connection] = None
    logs = []

    try:
        garmin_conn = open_ro(garmin_db_path)
        withings_conn = open_ro(withings_db_path)
        summary_conn = open_rw(summary_db_path)

        ensure_summary_schema(summary_conn)

        now_ts = now_iso()
        days_written = []
        days_skipped = []

        for day in dirty_periods:
            garmin = fetch_garmin_day(garmin_conn, day)
            withings = fetch_withings_day(withings_conn, day)

            if not garmin and not withings:
                days_skipped.append(day)
                logs.append(log("debug", f"skip {day}: no data in either source"))
                continue

            upsert_summary_day(summary_conn, day, garmin, withings, now_ts)
            days_written.append(day)

        summary_conn.commit()

        logs.append(log(
            "info",
            f"summarize: source={source} written={len(days_written)} "
            f"skipped={len(days_skipped)} days={days_written}",
        ))

        out_payload = {
            "source": source,
            "days_written": days_written,
            "days_skipped": days_skipped,
            "summarized_at": now_ts,
        }

        return ok_response(
            result=f"summarized: {len(days_written)} day(s) from {source}",
            events=[{"type": "health.summary_updated", "payload": out_payload}],
            state_updates=observe_summary_state(summary_conn),
            logs=logs,
        )

    except sqlite3.Error as exc:
        return error_response(f"Database error during summarize: {exc}", retry=True)
    except Exception as exc:  # noqa: BLE001
        return error_response(f"Summarize failed: {exc}", retry=True)
    finally:
        for conn in (garmin_conn, withings_conn, summary_conn):
            if conn is not None:
                conn.close()


def query_command(request: Dict[str, Any]) -> Dict[str, Any]:
    config = request.get("config") or {}
    summary_db_path = str(config.get("summary_db_path", "")).strip()
    if not summary_db_path:
        return error_response("Missing required config key: summary_db_path", retry=False)

    payload = extract_event_payload(request)
    start = str(payload.get("start", "")).strip()
    end = str(payload.get("end", "")).strip() or None

    if not start:
        return error_response("query requires payload.start (YYYY-MM-DD)", retry=False)

    conn: Optional[sqlite3.Connection] = None
    try:
        conn = open_ro(summary_db_path)

        if end:
            rows = conn.execute(
                "SELECT * FROM daily_health_summary WHERE day >= ? AND day <= ? ORDER BY day ASC",
                (start, end),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM daily_health_summary WHERE day >= ? ORDER BY day ASC",
                (start,),
            ).fetchall()

        col_names = [d[0] for d in conn.execute(
            "SELECT * FROM daily_health_summary LIMIT 0"
        ).description]

        records = [dict(zip(col_names, tuple(row))) for row in rows]

        out_payload = {
            "start": start,
            "end": end,
            "count": len(records),
            "records": records,
            "queried_at": now_iso(),
        }

        return ok_response(
            result=f"query: {len(records)} day(s) returned",
            events=[{"type": "health.query_result", "payload": out_payload}],
            logs=[log("info", f"query: start={start} end={end} count={len(records)}")],
        )

    except sqlite3.Error as exc:
        return error_response(f"Database error during query: {exc}", retry=False)
    except Exception as exc:  # noqa: BLE001
        return error_response(f"Query failed: {exc}", retry=False)
    finally:
        if conn is not None:
            conn.close()


def health_command(request: Dict[str, Any]) -> Dict[str, Any]:
    config = request.get("config") or {}
    summary_db_path = str(config.get("summary_db_path", "")).strip()
    if not summary_db_path:
        return error_response("Missing required config key: summary_db_path", retry=False)

    conn: Optional[sqlite3.Connection] = None
    try:
        init_conn = open_rw(summary_db_path)
        ensure_summary_schema(init_conn)
        init_conn.close()

        conn = open_ro(summary_db_path)
        row_count = conn.execute(
            "SELECT COUNT(*) AS c FROM daily_health_summary"
        ).fetchone()["c"]
        date_range = conn.execute(
            "SELECT MIN(day) AS min_day, MAX(day) AS max_day FROM daily_health_summary"
        ).fetchone()

        observed = observe_summary_state(conn)

        payload = {
            "healthy": True,
            "checked_at": now_iso(),
            "db": {
                "path": summary_db_path,
                "row_count": row_count,
                "min_day": date_range["min_day"],
                "max_day": date_range["max_day"],
            },
            "state": observed,
        }

        return ok_response(
            result=f"healthy: {row_count} days summarized, last={date_range['max_day']}",
            events=[{"type": "health_data_summary.health", "payload": payload}],
            logs=[log("info", "health check completed")],
        )

    except sqlite3.Error as exc:
        return error_response(f"Database error during health check: {exc}", retry=False)
    except Exception as exc:  # noqa: BLE001
        return error_response(f"Health check failed: {exc}", retry=False)
    finally:
        if conn is not None:
            conn.close()


def main() -> None:
    try:
        request = json.load(sys.stdin)
    except Exception as exc:  # noqa: BLE001
        json.dump(error_response(f"Invalid request JSON: {exc}", retry=False), sys.stdout)
        return

    command = str(request.get("command", "")).strip().lower()

    if command == "summarize":
        response = summarize_command(request)
    elif command == "query":
        response = query_command(request)
    elif command == "health":
        response = health_command(request)
    else:
        response = error_response(f"Unknown command: {command}", retry=False)

    json.dump(response, sys.stdout)


if __name__ == "__main__":
    main()
