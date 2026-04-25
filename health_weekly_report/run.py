#!/usr/bin/env python3
"""health_weekly_report — assemble a weekly health summary report.

Protocol v2. Reads JSON request from stdin, writes JSON response to stdout.

Pipeline:
  scheduled cron emits health_weekly_report.assemble request →
  assemble command reads daily_health_summary for an ISO week + N prior weeks →
  computes deterministic numeric briefs per section (sleep / recovery / activity /
  body / cardio / stress) → invokes fabric per section + once for exec_summary →
  assembles markdown + plain-text fallback → writes Obsidian archive copy →
  emits health_weekly_report.assembled with body_markdown, body_text, week_iso,
  archive_path for downstream email handoff.

Durable state (snapshot, presence-stable):
  latest_week_iso  : the week_iso of the most recent assembled archive file
  last_assembled_at: ISO-8601 timestamp of the most recent archive write

Both are observed from the filesystem, not action bookkeeping.
"""
from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

SECTIONS: Tuple[str, ...] = (
    "sleep",
    "recovery",
    "activity",
    "body",
    "cardio",
    "stress",
)

DEFAULT_TIMEZONE = "Australia/Sydney"
DEFAULT_PRIOR_WEEKS = 4
DEFAULT_SECTION_TIMEOUT_SEC = 90
DEFAULT_FABRIC_BIN = "fabric"
DEFAULT_PATTERN_SECTION = "health_section_narrative"
DEFAULT_PATTERN_EXEC = "health_exec_summary"


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


# ---------------------------------------------------------------------------
# Time / week boundary
# ---------------------------------------------------------------------------


def _tz(name: str):
    if ZoneInfo is None:
        return timezone.utc
    try:
        return ZoneInfo(name)
    except Exception:
        return timezone.utc


def resolve_week(
    *,
    week_iso: Optional[str] = None,
    target_date: Optional[str] = None,
    tz_name: str = DEFAULT_TIMEZONE,
    now: Optional[datetime] = None,
) -> Tuple[date, date, str]:
    """Return (monday_date, sunday_date, week_iso) for the requested week.

    Resolution rules:
      1. explicit week_iso ("2026-W17") wins
      2. else target_date ("2026-04-22") → ISO week containing that date
      3. else most recently *completed* ISO week relative to `now` in `tz_name`
    """
    if week_iso:
        try:
            year_str, w_str = week_iso.split("-W")
            year = int(year_str)
            week = int(w_str)
            monday = date.fromisocalendar(year, week, 1)
            sunday = date.fromisocalendar(year, week, 7)
        except ValueError as exc:
            raise ValueError(f"week_iso must be 'YYYY-Www' with a valid ISO week, got {week_iso!r}") from exc
        return monday, sunday, week_iso

    if target_date:
        try:
            d = date.fromisoformat(target_date)
        except ValueError as exc:
            raise ValueError(f"target_date must be 'YYYY-MM-DD', got {target_date!r}") from exc
    else:
        local_now = (now or datetime.now(timezone.utc)).astimezone(_tz(tz_name))
        today = local_now.date()
        d = today - timedelta(days=today.isoweekday())

    iso_year, iso_week, _ = d.isocalendar()
    monday = date.fromisocalendar(iso_year, iso_week, 1)
    sunday = date.fromisocalendar(iso_year, iso_week, 7)
    return monday, sunday, f"{iso_year:04d}-W{iso_week:02d}"


# ---------------------------------------------------------------------------
# summary.db access (read-only)
# ---------------------------------------------------------------------------


def open_summary_db_ro(db_path: str) -> sqlite3.Connection:
    p = Path(db_path).expanduser()
    if not p.exists():
        raise FileNotFoundError(f"summary.db not found at {p}")
    uri = f"file:{p}?mode=ro&immutable=1"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def fetch_week_rows(conn: sqlite3.Connection, monday: date, sunday: date) -> List[sqlite3.Row]:
    cur = conn.execute(
        "SELECT * FROM daily_health_summary "
        "WHERE day BETWEEN ? AND ? "
        "ORDER BY day ASC",
        (monday.isoformat(), sunday.isoformat()),
    )
    return cur.fetchall()


def fetch_prior_weeks(
    conn: sqlite3.Connection,
    monday: date,
    n: int,
) -> List[Dict[str, Any]]:
    """Return prior-N-week aggregates: [{week_iso, steps_avg, sleep_avg_min, ...}, ...]"""
    out: List[Dict[str, Any]] = []
    for i in range(1, n + 1):
        prior_monday = monday - timedelta(days=7 * i)
        prior_sunday = prior_monday + timedelta(days=6)
        cur = conn.execute(
            "SELECT "
            "  AVG(steps) AS steps_avg, "
            "  AVG(sleep_total_min) AS sleep_avg_min, "
            "  AVG(resting_hr) AS rhr_avg, "
            "  AVG(weight_kg) AS weight_avg "
            "FROM daily_health_summary "
            "WHERE day BETWEEN ? AND ?",
            (prior_monday.isoformat(), prior_sunday.isoformat()),
        )
        row = cur.fetchone()
        iso_y, iso_w, _ = prior_monday.isocalendar()
        out.append(
            {
                "week_iso": f"{iso_y:04d}-W{iso_w:02d}",
                "steps_avg": row["steps_avg"],
                "sleep_avg_min": row["sleep_avg_min"],
                "rhr_avg": row["rhr_avg"],
                "weight_avg": row["weight_avg"],
            }
        )
    out.reverse()
    return out


# ---------------------------------------------------------------------------
# Numeric briefs (deterministic, no LLM)
# ---------------------------------------------------------------------------


def _avg(values: Iterable[Optional[float]]) -> Optional[float]:
    nums = [v for v in values if v is not None]
    if not nums:
        return None
    return sum(nums) / len(nums)


def _delta(values: Iterable[Optional[float]]) -> Optional[float]:
    nums = [v for v in values if v is not None]
    if len(nums) < 2:
        return None
    return nums[-1] - nums[0]


def _round(v: Optional[float], n: int = 1) -> Optional[float]:
    if v is None:
        return None
    return round(v, n)


@dataclass
class SectionBrief:
    name: str
    headline: str
    facts: Dict[str, Any]
    days_with_data: int


def brief_sleep(rows: List[sqlite3.Row]) -> SectionBrief:
    total = [r["sleep_total_min"] for r in rows]
    deep = [r["sleep_deep_min"] for r in rows]
    light = [r["sleep_light_min"] for r in rows]
    rem = [r["sleep_rem_min"] for r in rows]
    score = [r["sleep_score"] for r in rows]
    days = sum(1 for v in total if v is not None)
    avg = _avg(total)
    headline = (
        f"Avg sleep {avg/60:.1f}h across {days}/{len(rows)} nights"
        if avg is not None
        else "No sleep data this week"
    )
    return SectionBrief(
        name="sleep",
        headline=headline,
        facts={
            "avg_total_min": _round(avg),
            "avg_deep_min": _round(_avg(deep)),
            "avg_light_min": _round(_avg(light)),
            "avg_rem_min": _round(_avg(rem)),
            "avg_score": _round(_avg(score)),
        },
        days_with_data=days,
    )


def brief_recovery(rows: List[sqlite3.Row]) -> SectionBrief:
    rhr = [r["resting_hr"] for r in rows]
    bb_max = [r["body_battery_max"] for r in rows]
    bb_min = [r["body_battery_min"] for r in rows]
    days = sum(1 for r in rows if r["resting_hr"] is not None or r["body_battery_max"] is not None)
    rhr_avg = _avg(rhr)
    bb_min_avg = _avg(bb_min)
    bb_max_avg = _avg(bb_max)
    if rhr_avg is not None and bb_min_avg is not None and bb_max_avg is not None:
        headline = f"Resting HR avg {rhr_avg:.0f} bpm; body battery range {bb_min_avg:.0f}–{bb_max_avg:.0f}"
    else:
        headline = "Insufficient recovery data this week"
    return SectionBrief(
        name="recovery",
        headline=headline,
        facts={
            "rhr_avg": _round(rhr_avg, 0),
            "rhr_delta": _round(_delta(rhr), 0),
            "body_battery_max_avg": _round(bb_max_avg, 0),
            "body_battery_min_avg": _round(bb_min_avg, 0),
        },
        days_with_data=days,
    )


def brief_activity(rows: List[sqlite3.Row]) -> SectionBrief:
    steps = [r["steps"] for r in rows]
    distance = [r["distance_km"] for r in rows]
    moderate = [r["moderate_activity_min"] for r in rows]
    vigorous = [r["vigorous_activity_min"] for r in rows]
    calories_active = [r["calories_active"] for r in rows]
    days = sum(1 for v in steps if v is not None)
    steps_avg = _avg(steps)
    distance_total = sum(v or 0 for v in distance)
    if steps_avg is not None:
        headline = f"Avg {steps_avg:,.0f} steps/day, {distance_total:.1f} km total"
    else:
        headline = "No activity data this week"
    return SectionBrief(
        name="activity",
        headline=headline,
        facts={
            "avg_steps": _round(steps_avg, 0),
            "total_distance_km": _round(distance_total, 1),
            "avg_moderate_min": _round(_avg(moderate), 0),
            "avg_vigorous_min": _round(_avg(vigorous), 0),
            "avg_calories_active": _round(_avg(calories_active), 0),
        },
        days_with_data=days,
    )


def brief_body(rows: List[sqlite3.Row]) -> SectionBrief:
    weight = [r["weight_kg"] for r in rows]
    fat_ratio = [r["fat_ratio"] for r in rows]
    muscle = [r["muscle_mass_kg"] for r in rows]
    days = sum(1 for v in weight if v is not None)
    weight_avg = _avg(weight)
    weight_delta = _delta(weight)
    if weight_avg is not None and weight_delta is not None:
        headline = f"Weight avg {weight_avg:.1f} kg (Δ {weight_delta:+.2f} kg over week)"
    elif weight_avg is not None:
        headline = f"Weight avg {weight_avg:.1f} kg (single reading)"
    else:
        headline = "No weight readings this week"
    return SectionBrief(
        name="body",
        headline=headline,
        facts={
            "weight_avg_kg": _round(weight_avg, 2),
            "weight_delta_kg": _round(weight_delta, 2),
            "fat_ratio_avg": _round(_avg(fat_ratio), 1),
            "fat_ratio_delta": _round(_delta(fat_ratio), 2),
            "muscle_avg_kg": _round(_avg(muscle), 2),
        },
        days_with_data=days,
    )


def brief_cardio(rows: List[sqlite3.Row]) -> SectionBrief:
    sys_bp = [r["systolic_bp"] for r in rows]
    dia_bp = [r["diastolic_bp"] for r in rows]
    hr_min = [r["hr_min"] for r in rows]
    hr_max = [r["hr_max"] for r in rows]
    days = sum(1 for v in sys_bp if v is not None)
    s = _avg(sys_bp)
    d = _avg(dia_bp)
    if s is not None and d is not None:
        headline = f"BP avg {s:.0f}/{d:.0f} mmHg ({days} readings)"
    else:
        headline = "No BP readings this week"
    return SectionBrief(
        name="cardio",
        headline=headline,
        facts={
            "systolic_avg": _round(s, 0),
            "diastolic_avg": _round(d, 0),
            "hr_min_avg": _round(_avg(hr_min), 0),
            "hr_max_avg": _round(_avg(hr_max), 0),
        },
        days_with_data=days,
    )


def brief_stress(rows: List[sqlite3.Row]) -> SectionBrief:
    stress = [r["stress_avg"] for r in rows]
    spo2 = [r["spo2_avg"] for r in rows]
    spo2_min = [r["spo2_min"] for r in rows]
    rr = [r["rr_avg"] for r in rows]
    days = sum(1 for r in rows if r["stress_avg"] is not None or r["spo2_avg"] is not None)
    s = _avg(stress)
    o = _avg(spo2)
    if s is not None and o is not None:
        headline = f"Stress avg {s:.0f}/100, SpO2 avg {o:.1f}%"
    elif s is not None:
        headline = f"Stress avg {s:.0f}/100"
    else:
        headline = "No stress / SpO2 data this week"
    return SectionBrief(
        name="stress",
        headline=headline,
        facts={
            "stress_avg": _round(s, 0),
            "spo2_avg": _round(o, 1),
            "spo2_min_avg": _round(_avg(spo2_min), 1),
            "rr_avg": _round(_avg(rr), 1),
        },
        days_with_data=days,
    )


BRIEF_FNS = {
    "sleep": brief_sleep,
    "recovery": brief_recovery,
    "activity": brief_activity,
    "body": brief_body,
    "cardio": brief_cardio,
    "stress": brief_stress,
}


# ---------------------------------------------------------------------------
# Sparkline
# ---------------------------------------------------------------------------


SPARK_CHARS = "▁▂▃▅▇"


def sparkline(values: List[Optional[float]]) -> str:
    nums = [v for v in values if v is not None]
    if not nums:
        return ""
    lo, hi = min(nums), max(nums)
    span = hi - lo or 1.0
    out = []
    for v in values:
        if v is None:
            out.append(" ")
        else:
            idx = int((v - lo) / span * (len(SPARK_CHARS) - 1))
            idx = max(0, min(len(SPARK_CHARS) - 1, idx))
            out.append(SPARK_CHARS[idx])
    return "".join(out)


# ---------------------------------------------------------------------------
# Fabric subprocess wrapper
# ---------------------------------------------------------------------------


def fabric_call(
    *,
    brief_text: str,
    pattern: str,
    model: Optional[str],
    fabric_bin: str,
    timeout_sec: int,
) -> Tuple[Optional[str], Optional[str]]:
    """Return (narrative, error). On success: (text, None). On failure: (None, reason)."""
    cmd = [fabric_bin, "--pattern", pattern]
    if model:
        cmd.extend(["--model", model])
    try:
        result = subprocess.run(
            cmd,
            input=brief_text,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
        )
    except FileNotFoundError:
        return None, f"fabric binary not found at '{fabric_bin}'"
    except subprocess.TimeoutExpired:
        return None, f"fabric '{pattern}' timed out after {timeout_sec}s"
    if result.returncode != 0:
        return None, f"fabric '{pattern}' exit {result.returncode}: {result.stderr.strip()[:200]}"
    out = result.stdout.strip()
    if not out:
        return None, f"fabric '{pattern}' produced empty output"
    return out, None


# ---------------------------------------------------------------------------
# Markdown assembly + plain-text derivation
# ---------------------------------------------------------------------------


def render_brief_table(brief: SectionBrief) -> str:
    lines = ["| Metric | Value |", "|---|---|"]
    for k, v in brief.facts.items():
        if v is None:
            lines.append(f"| {k} | — |")
        else:
            lines.append(f"| {k} | {v} |")
    return "\n".join(lines)


def render_prior_weeks_table(prior: List[Dict[str, Any]]) -> str:
    if not prior:
        return ""
    lines = ["| Week | Steps avg | Sleep avg (h) | RHR avg | Weight avg |", "|---|---|---|---|---|"]
    for w in prior:
        steps = f"{w['steps_avg']:,.0f}" if w["steps_avg"] is not None else "—"
        sleep_h = f"{w['sleep_avg_min']/60:.1f}" if w["sleep_avg_min"] is not None else "—"
        rhr = f"{w['rhr_avg']:.0f}" if w["rhr_avg"] is not None else "—"
        wt = f"{w['weight_avg']:.1f}" if w["weight_avg"] is not None else "—"
        lines.append(f"| {w['week_iso']} | {steps} | {sleep_h} | {rhr} | {wt} |")
    return "\n".join(lines)


def assemble_markdown(
    *,
    week_iso: str,
    monday: date,
    sunday: date,
    briefs: Dict[str, SectionBrief],
    narratives: Dict[str, Optional[str]],
    exec_summary: Optional[str],
    prior_weeks: List[Dict[str, Any]],
    rows: List[sqlite3.Row],
) -> str:
    parts: List[str] = []
    parts.append(f"# Weekly Health Report — {week_iso}")
    parts.append(f"_{monday.strftime('%a %d %b')} – {sunday.strftime('%a %d %b %Y')}_")
    parts.append("")

    parts.append("## Executive Summary")
    parts.append(exec_summary or "_No executive summary generated this week._")
    parts.append("")

    if prior_weeks:
        parts.append("## Trailing 4 Weeks")
        parts.append(render_prior_weeks_table(prior_weeks))
        parts.append("")

    for name in SECTIONS:
        b = briefs[name]
        parts.append(f"## {name.capitalize()}")
        parts.append(f"**{b.headline}**")
        parts.append("")
        parts.append(render_brief_table(b))
        parts.append("")
        narrative = narratives.get(name)
        if narrative:
            parts.append(narrative)
        else:
            parts.append("_Narrative skipped — numeric brief above is the source of truth._")
        parts.append("")

    spark_steps = sparkline([r["steps"] for r in rows])
    spark_sleep = sparkline([r["sleep_total_min"] for r in rows])
    spark_rhr = sparkline([r["resting_hr"] for r in rows])
    if spark_steps or spark_sleep or spark_rhr:
        parts.append("## Daily Sparklines")
        parts.append(f"- Steps:   `{spark_steps}`")
        parts.append(f"- Sleep:   `{spark_sleep}`")
        parts.append(f"- RHR:     `{spark_rhr}`")
        parts.append("")

    parts.append("---")
    parts.append(f"_Source: daily_health_summary; {len(rows)} day rows; assembled {now_iso()}_")
    return "\n".join(parts)


def markdown_to_plaintext(md: str) -> str:
    """Best-effort plain-text fallback. Preserves narratives, drops tables, strips heading hashes."""
    out: List[str] = []
    in_table = False
    for line in md.splitlines():
        stripped = line.strip()
        if stripped.startswith("|") and stripped.endswith("|"):
            in_table = True
            continue
        if in_table and not stripped:
            in_table = False
            continue
        if stripped.startswith("|---"):
            continue
        text = stripped
        while text.startswith("#"):
            text = text[1:].lstrip()
        text = text.replace("**", "").replace("`", "")
        out.append(text)
    # collapse triple-blank
    flat: List[str] = []
    blanks = 0
    for ln in out:
        if not ln:
            blanks += 1
            if blanks <= 1:
                flat.append(ln)
        else:
            blanks = 0
            flat.append(ln)
    return "\n".join(flat).strip() + "\n"


# ---------------------------------------------------------------------------
# Filesystem-observed snapshot state
# ---------------------------------------------------------------------------


def observe_report_state(obsidian_archive_dir: str) -> Dict[str, Any]:
    """Return observed state: latest_week_iso + last_assembled_at by filesystem."""
    p = Path(obsidian_archive_dir).expanduser()
    if not p.exists():
        return {"latest_week_iso": None, "last_assembled_at": None}
    latest_week_iso: Optional[str] = None
    latest_mtime: Optional[float] = None
    for f in p.glob("*.md"):
        stem = f.stem
        if "-W" in stem and len(stem) == 8:  # YYYY-Www
            mtime = f.stat().st_mtime
            if latest_mtime is None or mtime > latest_mtime:
                latest_mtime = mtime
                latest_week_iso = stem
    return {
        "latest_week_iso": latest_week_iso,
        "last_assembled_at": (
            datetime.fromtimestamp(latest_mtime, tz=timezone.utc).isoformat()
            if latest_mtime is not None
            else None
        ),
    }


def snapshot_state(observed: Dict[str, Any]) -> Dict[str, Any]:
    """Pure constructor — presence-stable two-key snapshot."""
    return {
        "latest_week_iso": observed.get("latest_week_iso"),
        "last_assembled_at": observed.get("last_assembled_at"),
    }


# ---------------------------------------------------------------------------
# assemble command
# ---------------------------------------------------------------------------


def assemble_command(config: Dict[str, Any], event: Dict[str, Any]) -> Dict[str, Any]:
    payload = event.get("payload", {}) if isinstance(event, dict) else {}

    summary_db_path = config.get("summary_db_path")
    obsidian_archive_dir = config.get("obsidian_archive_dir")
    if not summary_db_path:
        return error_response("Missing required config: summary_db_path", retry=False)
    if not obsidian_archive_dir:
        return error_response("Missing required config: obsidian_archive_dir", retry=False)

    fabric_bin = config.get("fabric_bin", DEFAULT_FABRIC_BIN)
    model = config.get("model")
    pattern_section = config.get("fabric_pattern_section", DEFAULT_PATTERN_SECTION)
    pattern_exec = config.get("fabric_pattern_exec_summary", DEFAULT_PATTERN_EXEC)
    try:
        timeout_sec = int(config.get("section_timeout_sec", DEFAULT_SECTION_TIMEOUT_SEC))
        prior_weeks_n = int(config.get("prior_weeks", DEFAULT_PRIOR_WEEKS))
    except (TypeError, ValueError) as exc:
        return error_response(f"Non-numeric config value: {exc}", retry=False)
    tz_name = config.get("timezone", DEFAULT_TIMEZONE)
    dry_run = bool(payload.get("dry_run", False))

    try:
        monday, sunday, week_iso = resolve_week(
            week_iso=payload.get("week_iso"),
            target_date=payload.get("target_date"),
            tz_name=tz_name,
        )
    except ValueError as exc:
        return error_response(str(exc), retry=False)

    try:
        conn = open_summary_db_ro(summary_db_path)
    except FileNotFoundError as exc:
        return error_response(str(exc), retry=True)
    except sqlite3.Error as exc:
        return error_response(f"Failed to open summary.db: {exc}", retry=True)

    try:
        rows = fetch_week_rows(conn, monday, sunday)
        prior = fetch_prior_weeks(conn, monday, prior_weeks_n)
    finally:
        conn.close()

    briefs: Dict[str, SectionBrief] = {name: BRIEF_FNS[name](rows) for name in SECTIONS}

    narratives: Dict[str, Optional[str]] = {}
    sections_completed: List[str] = []
    sections_skipped: List[str] = []
    fabric_logs: List[Dict[str, str]] = []
    for name in SECTIONS:
        b = briefs[name]
        brief_text = (
            f"Section: {name}\nWeek: {week_iso} ({monday} – {sunday})\n"
            f"Headline: {b.headline}\n"
            f"Days with data: {b.days_with_data}/{len(rows)}\n"
            f"Facts:\n"
            + "\n".join(f"  {k}: {v}" for k, v in b.facts.items())
        )
        narrative, err = fabric_call(
            brief_text=brief_text,
            pattern=pattern_section,
            model=model,
            fabric_bin=fabric_bin,
            timeout_sec=timeout_sec,
        )
        if narrative:
            narratives[name] = narrative
            sections_completed.append(name)
        else:
            narratives[name] = None
            sections_skipped.append(name)
            fabric_logs.append(log("warn", f"section {name} narrative skipped: {err}"))

    exec_summary: Optional[str] = None
    if any(narratives.values()):
        exec_input_lines = [
            f"Week: {week_iso} ({monday} – {sunday})",
            "Section headlines:",
        ]
        for name in SECTIONS:
            exec_input_lines.append(f"  - {name}: {briefs[name].headline}")
        exec_input_lines.append("\nSection narratives:")
        for name, narr in narratives.items():
            if narr:
                exec_input_lines.append(f"\n[{name}]\n{narr[:1500]}")
        exec_input = "\n".join(exec_input_lines)
        exec_summary, err = fabric_call(
            brief_text=exec_input,
            pattern=pattern_exec,
            model=model,
            fabric_bin=fabric_bin,
            timeout_sec=timeout_sec,
        )
        if exec_summary is None:
            fabric_logs.append(log("warn", f"exec_summary skipped: {err}"))

    body_markdown = assemble_markdown(
        week_iso=week_iso,
        monday=monday,
        sunday=sunday,
        briefs=briefs,
        narratives=narratives,
        exec_summary=exec_summary,
        prior_weeks=prior,
        rows=rows,
    )
    body_text = markdown_to_plaintext(body_markdown)

    archive_dir = Path(obsidian_archive_dir).expanduser()
    archive_path = archive_dir / f"{week_iso}.md"
    if not dry_run:
        try:
            archive_dir.mkdir(parents=True, exist_ok=True)
            archive_path.write_text(body_markdown, encoding="utf-8")
        except OSError as exc:
            return error_response(f"Failed to write archive {archive_path}: {exc}", retry=True)

    observed = observe_report_state(str(archive_dir))
    state = snapshot_state(observed)

    event_payload = {
        "week_iso": week_iso,
        "period_start": monday.isoformat(),
        "period_end": sunday.isoformat(),
        "body_markdown": body_markdown,
        "body_text": body_text,
        "archive_path": str(archive_path),
        "sections_completed": sections_completed,
        "sections_skipped": sections_skipped,
        "source_type": "llm" if any(narratives.values()) else "deterministic",
    }

    result_msg = (
        f"Assembled {week_iso}: {len(sections_completed)}/{len(SECTIONS)} narratives, "
        f"{len(rows)} day rows, {'dry-run' if dry_run else 'archive=' + str(archive_path)}"
    )
    return ok_response(
        result=result_msg,
        events=[{"type": "health_weekly_report.assembled", "payload": event_payload}],
        state_updates=state,
        logs=[log("info", result_msg)] + fabric_logs,
    )


# ---------------------------------------------------------------------------
# health command
# ---------------------------------------------------------------------------


def health_command(config: Dict[str, Any]) -> Dict[str, Any]:
    summary_db_path = config.get("summary_db_path")
    obsidian_archive_dir = config.get("obsidian_archive_dir")
    fabric_bin = config.get("fabric_bin", DEFAULT_FABRIC_BIN)

    db_ok = False
    db_msg = "summary_db_path missing"
    if summary_db_path:
        p = Path(summary_db_path).expanduser()
        if p.exists():
            try:
                conn = open_summary_db_ro(str(p))
                conn.execute("SELECT 1 FROM daily_health_summary LIMIT 1").fetchall()
                conn.close()
                db_ok = True
                db_msg = f"readable: {p}"
            except sqlite3.Error as exc:
                db_msg = f"open failed: {exc}"
        else:
            db_msg = f"not found: {p}"

    archive_ok = False
    archive_msg = "obsidian_archive_dir missing"
    if obsidian_archive_dir:
        ap = Path(obsidian_archive_dir).expanduser()
        # writable if dir exists and we can create, or parent exists
        target = ap if ap.exists() else ap.parent
        archive_ok = target.exists() and os.access(target, os.W_OK)
        archive_msg = f"{'writable' if archive_ok else 'not writable'}: {ap}"

    fabric_ok = False
    fabric_msg = ""
    try:
        result = subprocess.run(
            [fabric_bin, "--version"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        fabric_ok = result.returncode == 0
        fabric_msg = result.stdout.strip() if fabric_ok else f"exit {result.returncode}"
    except FileNotFoundError:
        fabric_msg = f"not found at '{fabric_bin}'"
    except subprocess.TimeoutExpired:
        fabric_msg = "version probe timed out"

    overall_ok = db_ok and archive_ok and fabric_ok
    observed = observe_report_state(obsidian_archive_dir or "")
    payload = {
        "summary_db": {"ok": db_ok, "detail": db_msg},
        "obsidian_archive": {"ok": archive_ok, "detail": archive_msg},
        "fabric": {"ok": fabric_ok, "detail": fabric_msg},
        "state": observed,
        "checked_at": now_iso(),
    }

    response: Dict[str, Any] = {
        "status": "ok" if overall_ok else "error",
        "result": (
            "health_weekly_report healthy"
            if overall_ok
            else f"unhealthy: db={db_ok} archive={archive_ok} fabric={fabric_ok}"
        ),
        "events": [{"type": "health_weekly_report.health", "payload": payload}],
        "logs": [
            log("info" if overall_ok else "warn", f"health: db={db_msg}; archive={archive_msg}; fabric={fabric_msg}"),
        ],
    }
    if not overall_ok:
        response["error"] = response["result"]
        response["retry"] = False
    return response


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def main() -> None:
    try:
        request = json.load(sys.stdin)
    except json.JSONDecodeError as exc:
        json.dump(error_response(f"invalid request JSON: {exc}", retry=False), sys.stdout)
        return

    command = request.get("command", "")
    config = request.get("config", {}) or {}
    event = request.get("event", {}) or {}

    if command == "assemble":
        response = assemble_command(config, event)
    elif command == "health":
        response = health_command(config)
    else:
        response = error_response(f"Unknown command: {command!r}", retry=False)

    json.dump(response, sys.stdout)


if __name__ == "__main__":
    main()
