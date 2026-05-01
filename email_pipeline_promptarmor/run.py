#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyyaml>=6.0"]
# ///
"""email_pipeline_promptarmor — Ductile plugin (protocol v2).

Regex scorer using prompt-armor's default_rules.yml (63 weighted rules,
Apache 2.0). Consumes sanitised text from durable baggage
(context.mail.parts), runs each rule's pattern, computes a verdict, emits
email.scored.regex with verdict + per-rule hits.

Verdict logic (per pipeline brief §Stage 3 Regex scorer):
  - Any rule with category "data_exfiltration" matches → BLOCK (instant)
  - Any rule with weight >= block_threshold matches → BLOCK
  - Otherwise → PASS

Hard-fail discipline: missing baggage, bad rules file, or scoring budget
exceeded → status: error with retry: false. Per-rule timeouts log + skip
the offending rule, not the run.

Event consumed: pipeline-routed after email_pipeline_sanitise.
  context.mail.message_id  — Gmail Message-ID
  context.mail.parts       — list of {mime_type, sanitised_text, ...}

Event emitted: email.scored.regex
  payload.message_id      — pass-through
  payload.verdict         — "block" | "pass"
  payload.max_weight      — float; 0.0 if no hits
  payload.categories      — list of distinct categories that fired
  payload.hits            — list of {rule_id, category, weight, matched, span}
  payload.block_reason    — "data_exfiltration_instant" | "weight_threshold" | null

Config keys (all optional):
  block_threshold       (float, default 0.85)
  per_rule_timeout_ms   (int, default 200)
  total_budget_ms       (int, default 5000)
"""

from __future__ import annotations

import concurrent.futures
import json
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, NotRequired, TypedDict, cast

import yaml

EVENT_TYPE = "email.scored.regex"
DEDUPE_PREFIX = "regex-scored:msg:"
CATEGORY_INSTANT_BLOCK = "data_exfiltration"
DEFAULT_BLOCK_THRESHOLD = 0.85
DEFAULT_PER_RULE_MS = 200
DEFAULT_TOTAL_BUDGET_MS = 5000
CMD_HANDLE = "handle"
CMD_HEALTH = "health"


# ── protocol shapes ───────────────────────────────────────────────────────────


class LogEntry(TypedDict):
    level: str
    message: str


class ScoringHit(TypedDict):
    rule_id: str
    category: str
    weight: float
    matched: str
    span: list[int]


class RegexScorePayload(TypedDict):
    message_id: str
    verdict: str
    max_weight: float
    categories: list[str]
    hits: list[ScoringHit]
    block_reason: str | None


class RegexScoreEvent(TypedDict):
    type: str
    payload: RegexScorePayload
    dedupe_key: str


class ResponseOk(TypedDict):
    status: str
    result: str
    logs: list[LogEntry]
    events: NotRequired[list[RegexScoreEvent]]


class ResponseErr(TypedDict):
    status: str
    error: str
    retry: bool
    logs: list[LogEntry]


# ── response helpers ──────────────────────────────────────────────────────────


def ok(
    result: str,
    *,
    logs: list[LogEntry] | None = None,
    events: list[RegexScoreEvent] | None = None,
) -> ResponseOk:
    resp: ResponseOk = {
        "status": "ok",
        "result": result,
        "logs": logs or [{"level": "info", "message": result}],
    }
    if events:
        resp["events"] = events
    return resp


def err(
    message: str,
    *,
    retry: bool = False,
    logs: list[LogEntry] | None = None,
) -> ResponseErr:
    return {
        "status": "error",
        "error": message,
        "retry": retry,
        "logs": logs or [{"level": "error", "message": message}],
    }


# ── rule loading + compilation ────────────────────────────────────────────────


@dataclass(slots=True, frozen=True)
class CompiledRule:
    rule_id: str
    category: str
    weight: float
    description: str
    pattern: re.Pattern[str]


def load_rules(path: str | Path) -> list[CompiledRule]:
    """Load + compile rules from YAML.

    Raises RuntimeError naming the offending rule_id on bad pattern or weight.
    """
    text = Path(path).read_text(encoding="utf-8")
    data = yaml.safe_load(text)

    raw = (data or {}).get("rules") or []
    if not raw:
        raise RuntimeError(f"rules file {str(path)!r} contains no 'rules' list")

    out: list[CompiledRule] = []
    for entry in raw:
        rid = str(entry.get("id", "")).strip()
        if not rid:
            raise RuntimeError(f"rule with no id in {str(path)!r}: {entry!r}")
        try:
            pat = re.compile(entry["pattern"], re.IGNORECASE)
        except re.error as exc:
            raise RuntimeError(f"rule {rid!r} pattern compile failed: {exc}") from exc
        try:
            weight = float(entry.get("weight", 0.0))
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"rule {rid!r} weight not numeric: {exc}") from exc
        out.append(
            CompiledRule(
                rule_id=rid,
                category=str(entry.get("category", "")).strip(),
                weight=weight,
                description=str(entry.get("description", "")),
                pattern=pat,
            )
        )
    return out


def _default_rules_path() -> Path:
    return Path(__file__).resolve().parent / "default_rules.yml"


# Load + compile at module import. Capture failures so cmd_handle/cmd_health
# can return a structured error response instead of an opaque non-zero exit.
_RULES: list[CompiledRule] = []
_RULES_LOAD_ERROR: str | None = None
try:
    _RULES = load_rules(_default_rules_path())
except Exception as _exc:
    # BLE001 justification: any rule-load failure must surface via cmd_handle/cmd_health
    # as a structured error response. Letting the exception escape here would crash the
    # plugin process before stdin can be read, leaving ductile with an opaque non-zero exit.
    _RULES_LOAD_ERROR = str(_exc)


# ── scoring ───────────────────────────────────────────────────────────────────


_SENTINEL_TIMEOUT = object()


def _search_via_pool(
    pool: concurrent.futures.ThreadPoolExecutor,
    pattern: re.Pattern[str],
    text: str,
    timeout_s: float,
) -> re.Match[str] | None | object:
    """Run pattern.search(text) with a wall-clock budget via a shared pool.

    Returns the Match (or None for clean no-match), or _SENTINEL_TIMEOUT on timeout.
    Note: thread-based timeout cannot truly cancel a backtracking re.search; on
    timeout the worker thread keeps running until the regex returns. We DO NOT
    block the plugin response on it — the future is left orphaned, the pool is
    abandoned, and the process exits naturally on plugin completion.
    """
    fut = pool.submit(pattern.search, text)
    try:
        return fut.result(timeout=timeout_s)
    except concurrent.futures.TimeoutError:
        return _SENTINEL_TIMEOUT


def score(
    text: str,
    rules: list[CompiledRule],
    *,
    block_threshold: float,
    per_rule_timeout_ms: int,
    total_budget_ms: int,
) -> tuple[list[ScoringHit], list[LogEntry], bool]:
    """Run rules over text. Returns (hits, logs, budget_exceeded)."""
    hits: list[ScoringHit] = []
    timed_out: list[str] = []
    deadline = time.monotonic() + (total_budget_ms / 1000.0)
    per_rule_s = per_rule_timeout_ms / 1000.0

    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        for rule in rules:
            if time.monotonic() >= deadline:
                return hits, _build_logs(timed_out), True
            m = _search_via_pool(pool, rule.pattern, text, per_rule_s)
            if m is _SENTINEL_TIMEOUT:
                timed_out.append(rule.rule_id)
                continue
            if m is None:
                continue
            match = cast(re.Match[str], m)
            hits.append(
                {
                    "rule_id": rule.rule_id,
                    "category": rule.category,
                    "weight": rule.weight,
                    "matched": match.group(0)[:200],
                    "span": [match.start(), match.end()],
                }
            )
    finally:
        pool.shutdown(wait=False)

    return hits, _build_logs(timed_out), False


def _build_logs(timed_out: list[str]) -> list[LogEntry]:
    if not timed_out:
        return []
    return [{"level": "warn", "message": f"per-rule timeout (skipped): {timed_out}"}]


def verdict(
    hits: list[ScoringHit], block_threshold: float
) -> tuple[str, str | None, float, list[str]]:
    if not hits:
        return "pass", None, 0.0, []
    categories = sorted({h["category"] for h in hits})
    max_weight = max(h["weight"] for h in hits)
    if any(h["category"] == CATEGORY_INSTANT_BLOCK for h in hits):
        return "block", "data_exfiltration_instant", max_weight, categories
    if max_weight >= block_threshold:
        return "block", "weight_threshold", max_weight, categories
    return "pass", None, max_weight, categories


# ── handle ────────────────────────────────────────────────────────────────────


def cmd_handle(config: dict[str, Any], context: dict[str, Any]) -> ResponseOk | ResponseErr:
    if _RULES_LOAD_ERROR is not None:
        return err(f"rules failed to load at import: {_RULES_LOAD_ERROR}")

    block_threshold = (
        float(config["block_threshold"]) if "block_threshold" in config else DEFAULT_BLOCK_THRESHOLD
    )
    per_rule_timeout_ms = (
        int(config["per_rule_timeout_ms"])
        if "per_rule_timeout_ms" in config
        else DEFAULT_PER_RULE_MS
    )
    total_budget_ms = (
        int(config["total_budget_ms"]) if "total_budget_ms" in config else DEFAULT_TOTAL_BUDGET_MS
    )

    mail = context.get("mail") if isinstance(context, dict) else None
    if not isinstance(mail, dict):
        return err("context.mail is missing or not an object")

    msg_id = mail.get("message_id")
    if not msg_id:
        return err("context.mail.message_id is missing")

    parts = mail.get("parts")
    if not isinstance(parts, list):
        return err("context.mail.parts is missing or not a list")

    text = "\n\n".join(str(p.get("sanitised_text", "")) for p in parts if isinstance(p, dict))

    hits, score_logs, budget_exceeded = score(
        text,
        _RULES,
        block_threshold=block_threshold,
        per_rule_timeout_ms=per_rule_timeout_ms,
        total_budget_ms=total_budget_ms,
    )

    if budget_exceeded:
        return err(
            f"scoring budget {total_budget_ms}ms exceeded after {len(hits)} hit(s)",
            retry=False,
            logs=score_logs,
        )

    v, reason, max_w, categories = verdict(hits, block_threshold)

    payload: RegexScorePayload = {
        "message_id": str(msg_id),
        "verdict": v,
        "max_weight": max_w,
        "categories": categories,
        "hits": hits,
        "block_reason": reason,
    }

    summary = (
        f"{v.upper()} {msg_id}: {len(hits)} hit(s), "
        f"max_weight={max_w:.2f}, categories={categories or '[]'}"
    )
    if reason:
        summary += f", reason={reason}"

    return ok(
        summary,
        logs=[*score_logs, {"level": "info", "message": summary}],
        events=[
            {
                "type": EVENT_TYPE,
                "payload": payload,
                "dedupe_key": f"{DEDUPE_PREFIX}{msg_id}",
            }
        ],
    )


# ── health ────────────────────────────────────────────────────────────────────


def cmd_health(config: dict[str, Any]) -> ResponseOk | ResponseErr:
    if _RULES_LOAD_ERROR is not None:
        return err(f"rules failed to load at import: {_RULES_LOAD_ERROR}")

    rule_count = len(_RULES)
    categories = sorted({r.category for r in _RULES})
    msg = (
        f"email_pipeline_promptarmor healthy — {rule_count} rules loaded "
        f"across {len(categories)} categor{'y' if len(categories) == 1 else 'ies'}: "
        f"{', '.join(categories)}"
    )
    return ok(msg)


# ── entrypoint ────────────────────────────────────────────────────────────────


def main() -> None:
    try:
        req = json.load(sys.stdin)
    except json.JSONDecodeError as exc:
        json.dump(err(f"Invalid JSON input: {exc}"), sys.stdout)
        sys.stdout.write("\n")
        sys.exit(1)

    command = req.get("command", "")
    config = req.get("config", {})
    if not isinstance(config, dict):
        config = {}
    context = req.get("context", {}) if isinstance(req, dict) else {}
    if not isinstance(context, dict):
        context = {}

    out: ResponseOk | ResponseErr
    if command == CMD_HANDLE:
        out = cmd_handle(config, context)
    elif command == CMD_HEALTH:
        out = cmd_health(config)
    else:
        out = err(f"Unknown command: {command!r}")

    json.dump(out, sys.stdout)
    sys.stdout.write("\n")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
