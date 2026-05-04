#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11,<3.13"
# dependencies = []
# ///
"""email_pipeline_sentinel — Ductile plugin (protocol v2).

Second independent scorer in the ductile email pipeline. Delegates
classification to the Sentinel v2 HTTP service (sentinel-service
running on Unraid). Sends each email part as a POST /score request,
aggregates per-part scores, emits email.scored.sentinel.

Verdict logic:
  - Any part with decision "block" → verdict "block"
  - Otherwise → verdict "pass"
  - payload.score = max score across parts (0.0 if no parts)

Hard-fail discipline: missing baggage, HTTP error, or non-200 response
→ status: error with retry: false.

Event consumed: pipeline-routed after email_pipeline_promptarmor.
  context.mail.message_id  — Gmail Message-ID
  context.mail.parts       — list of {mime_type, sanitised_text, ...}

Event emitted: email.scored.sentinel
  payload.message_id        — pass-through
  payload.verdict           — "block" | "pass"
  payload.score             — float; max across parts (0.0 if no parts)
  payload.decision          — decision string for max-score part
  payload.reason            — reason string for max-score part
  payload.per_part_scores   — list of {part_index, score, decision, reason}
  dedupe_key                — sentinel-scored:msg:<message_id>

Config keys (all optional):
  endpoint_url        (str, default http://192.168.20.4:11442/score)
  request_timeout_s   (int, default 30)
  per_part_max_chars  (int, default 32768) — truncate long parts
"""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request
from typing import Any, NotRequired, TypedDict

EVENT_TYPE = "email.scored.sentinel"
DEDUPE_PREFIX = "sentinel-scored:msg:"
DEFAULT_ENDPOINT = "http://192.168.20.4:11442/score"
DEFAULT_HEALTH_ENDPOINT = "http://192.168.20.4:11442/health"
DEFAULT_TIMEOUT_S = 30
DEFAULT_PER_PART_MAX_CHARS = 32768
REASON_NO_PARTS = "no_parts_to_scan"
CMD_HANDLE = "handle"
CMD_HEALTH = "health"


# ── protocol shapes ───────────────────────────────────────────────────────────


class LogEntry(TypedDict):
    level: str
    message: str


class PerPartScore(TypedDict):
    part_index: int
    score: float
    decision: str
    reason: str


class SentinelScorePayload(TypedDict):
    message_id: str
    verdict: str
    score: float
    decision: str
    reason: str
    per_part_scores: list[PerPartScore]


class SentinelEvent(TypedDict):
    type: str
    payload: SentinelScorePayload
    dedupe_key: str


class ResponseOk(TypedDict):
    status: str
    result: str
    logs: list[LogEntry]
    events: NotRequired[list[SentinelEvent]]


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
    events: list[SentinelEvent] | None = None,
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


# ── HTTP scoring ──────────────────────────────────────────────────────────────


def _score_text(
    text: str,
    endpoint: str,
    timeout_s: int,
) -> tuple[float, str, str]:
    """POST text to the Sentinel service. Returns (score, decision, reason)."""
    payload = json.dumps({"text": text}).encode("utf-8")
    req = urllib.request.Request(  # nosec B310
        endpoint,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:  # nosec B310
            body = json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"HTTP {exc.code} from sentinel service: {exc.reason}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Cannot reach sentinel service at {endpoint}: {exc.reason}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Non-JSON response from sentinel service: {exc}") from exc

    return float(body["score"]), str(body["decision"]), str(body["reason"])


# ── handle ────────────────────────────────────────────────────────────────────


def cmd_handle(config: dict[str, Any], context: dict[str, Any]) -> ResponseOk | ResponseErr:
    endpoint = str(config.get("endpoint_url") or DEFAULT_ENDPOINT)
    timeout_s = int(config.get("request_timeout_s") or DEFAULT_TIMEOUT_S)
    per_part_max_chars = int(config.get("per_part_max_chars") or DEFAULT_PER_PART_MAX_CHARS)

    mail = context.get("mail") if isinstance(context, dict) else None
    if not isinstance(mail, dict):
        return err("context.mail is missing or not an object")

    msg_id = mail.get("message_id")
    if not msg_id:
        return err("context.mail.message_id is missing")

    parts = mail.get("parts")
    if not isinstance(parts, list):
        return err("context.mail.parts is missing or not a list")

    per_part: list[PerPartScore] = []
    truncated_parts: list[int] = []
    max_score = 0.0
    top_decision = "allow"
    top_reason = REASON_NO_PARTS
    saw_block = False
    have_top = False

    for idx, part in enumerate(parts):
        if not isinstance(part, dict):
            continue
        text = str(part.get("sanitised_text", ""))
        if not text:
            continue
        if len(text) > per_part_max_chars:
            text = text[:per_part_max_chars]
            truncated_parts.append(idx)
        try:
            score, decision, reason = _score_text(text, endpoint, timeout_s)
        except RuntimeError as exc:
            return err(str(exc), retry=False)

        per_part.append({"part_index": idx, "score": score, "decision": decision, "reason": reason})
        if not have_top or score > max_score:
            max_score = score
            top_decision = decision
            top_reason = reason
            have_top = True
        if decision == "block":
            saw_block = True

    verdict = "block" if saw_block else "pass"

    payload: SentinelScorePayload = {
        "message_id": str(msg_id),
        "verdict": verdict,
        "score": max_score,
        "decision": top_decision,
        "reason": top_reason,
        "per_part_scores": per_part,
    }

    summary = (
        f"{verdict.upper()} {msg_id}: {len(per_part)} part(s) scanned, "
        f"max_score={max_score:.3f}, decision={top_decision}"
    )
    logs: list[LogEntry] = [{"level": "info", "message": summary}]
    if truncated_parts:
        logs.append({
            "level": "warn",
            "message": f"truncated parts to {per_part_max_chars} chars: {truncated_parts}",
        })

    return ok(
        summary,
        logs=logs,
        events=[{
            "type": EVENT_TYPE,
            "payload": payload,
            "dedupe_key": f"{DEDUPE_PREFIX}{msg_id}",
        }],
    )


# ── health ────────────────────────────────────────────────────────────────────


def cmd_health(config: dict[str, Any]) -> ResponseOk | ResponseErr:
    health_url = str(config.get("health_url") or DEFAULT_HEALTH_ENDPOINT)
    timeout_s = int(config.get("request_timeout_s") or DEFAULT_TIMEOUT_S)
    try:
        with urllib.request.urlopen(health_url, timeout=timeout_s) as resp:  # nosec B310
            body = json.loads(resp.read())
    except (urllib.error.URLError, urllib.error.HTTPError) as exc:
        return err(f"sentinel service unreachable at {health_url}: {exc}", retry=False)
    except json.JSONDecodeError as exc:
        return err(f"non-JSON health response: {exc}", retry=False)

    if body.get("status") != "ok":
        return err(f"sentinel service unhealthy: {body}", retry=False)

    model = body.get("model", "unknown")
    return ok(f"email_pipeline_sentinel healthy — service reports model={model}")


# ── entrypoint ────────────────────────────────────────────────────────────────


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
    context = req.get("context", {})
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
