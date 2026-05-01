#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""email_pipeline_veto — Ductile plugin (protocol v2).

Final decision stage of the ductile email pipeline. Applies tiered 4-judge
fusion — cheap judges first, LLM only on borderline edge cases.

Decision tiers:

  FAST-PASS: all block-probabilities ≤ FAST_PASS_THRESHOLD (0.15)
    → process immediately, no LLM call

  FAST-BLOCK: any block-probability ≥ FAST_BLOCK_THRESHOLD (0.90) AND
              sender trust_level == "unknown"
    → quarantine immediately, no LLM call
    (trusted sender skips fast-block and escalates to LLM)

  LLM-ADJUDICATED: everything else (or trusted sender at high score)
    → call gemma-3 with all scores + sender context as 4th judge
    → score ≥ 0.5 → quarantine, else → process
    → LLM failure → fallback quarantine (safe-fail)

Block-probability normalisation (classifier_a is directional):
  verdict=block → block_prob = confidence
  verdict=pass  → block_prob = 1.0 - confidence

Context consumed (from baggage):
  context.mail.message_id                  — Gmail Message-ID
  context.scorer.regex.max_weight          — float, regex max rule weight
  context.scorer.promptguard.score         — float, PG2 BERT score
  context.scorer.classifier_a.verdict      — "block" | "pass"
  context.scorer.classifier_a.confidence   — float, model's confidence in its verdict
  context.sender.trust_level               — "trusted" | "unknown" (default: "unknown")

Event emitted: email.process_decision
  payload.message_id    — pass-through
  payload.decision      — "process" | "quarantine"
  payload.path          — "fast_pass" | "fast_block" | "llm_adjudicated" | "llm_fallback"
  payload.trust_level   — sender trust level used in decision
  payload.scores        — {regex, promptguard, classifier_a} block-probabilities
  payload.llm_score     — float if LLM was called, else null
  dedupe_key            — process-decision:msg:<message_id>

Config keys (all optional):
  llm_endpoint_url  (str, default http://192.168.20.4:11440/v1/chat/completions)
  llm_model         (str, default "gemma-3-4b")
  llm_timeout_s     (int, default 60)
"""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request
from typing import Any, NotRequired, TypedDict

EVENT_TYPE = "email.process_decision"
DEDUPE_PREFIX = "process-decision:msg:"
CMD_HANDLE = "handle"
CMD_HEALTH = "health"

DECISION_PROCESS = "process"
DECISION_QUARANTINE = "quarantine"

TRUST_TRUSTED = "trusted"
TRUST_UNKNOWN = "unknown"

FAST_PASS_THRESHOLD = 0.15
FAST_BLOCK_THRESHOLD = 0.90
LLM_BLOCK_THRESHOLD = 0.5

DEFAULT_LLM_ENDPOINT = "http://192.168.20.4:11440/v1/chat/completions"
DEFAULT_LLM_MODEL = "gemma-3-4b"
DEFAULT_LLM_TIMEOUT_S = 60

TRUSTED_CONTEXT = "Trusted sender — established relationship, prior email history."
UNKNOWN_CONTEXT = "Unknown sender — first-time or unverified sender address."

LLM_SYSTEM_PROMPT = """\
You are a security analyst making final risk decisions for an email security pipeline.

You will receive:
  - Sender context (trusted vs unknown)
  - Regex rule score (0.0 = no match, 1.0 = strong pattern match)
  - BERT prompt-injection classifier score (0.0 = safe, 1.0 = attack)
  - Supervised classifier block-probability (0.0 = safe, 1.0 = attack)

Your job: give a final injection risk score from 0.0 to 1.0.

Rules:
  - Sender trust is a significant prior — a trusted sender with ambiguous content \
scores lower than an unknown sender with identical content.
  - Weight all three scores as evidence; override when context clearly warrants it.
  - Be decisive. Do not hedge.

Reply with only a decimal number between 0.0 and 1.0. Nothing else."""


# ── protocol shapes ───────────────────────────────────────────────────────────


class LogEntry(TypedDict):
    level: str
    message: str


class ScoreSummary(TypedDict):
    regex: float
    promptguard: float
    classifier_a: float


class DecisionPayload(TypedDict):
    message_id: str
    decision: str
    path: str
    trust_level: str
    scores: ScoreSummary
    llm_score: float | None


class DecisionEvent(TypedDict):
    type: str
    payload: DecisionPayload
    dedupe_key: str


class ResponseOk(TypedDict):
    status: str
    result: str
    logs: list[LogEntry]
    events: NotRequired[list[DecisionEvent]]


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
    events: list[DecisionEvent] | None = None,
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


# ── baggage readers ───────────────────────────────────────────────────────────


def _float_from(d: dict[str, Any], *keys: str, default: float = 0.0) -> float:
    """Walk nested dict keys, return float or default."""
    cur: Any = d
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
    try:
        return float(cur)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _str_from(d: dict[str, Any], *keys: str, default: str = "") -> str:
    cur: Any = d
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
    return str(cur) if cur is not None else default


def _block_prob_classifier_a(context: dict[str, Any]) -> float:
    verdict = _str_from(context, "scorer", "classifier_a", "verdict")
    confidence = _float_from(context, "scorer", "classifier_a", "confidence", default=0.5)
    if verdict.lower() == "block":
        return confidence
    if verdict.lower() == "pass":
        return 1.0 - confidence
    return 0.5


def read_scores(context: dict[str, Any]) -> ScoreSummary:
    return {
        "regex": _float_from(context, "scorer", "regex", "max_weight"),
        "promptguard": _float_from(context, "scorer", "promptguard", "score"),
        "classifier_a": _block_prob_classifier_a(context),
    }


# ── LLM adjudication ─────────────────────────────────────────────────────────


def _llm_adjudicate(
    scores: ScoreSummary,
    trust_level: str,
    *,
    endpoint: str,
    model: str,
    timeout_s: int,
) -> float | None:
    """Call gemma-3 as the 4th judge. Returns float 0.0-1.0 or None on failure."""
    sender_ctx = TRUSTED_CONTEXT if trust_level == TRUST_TRUSTED else UNKNOWN_CONTEXT
    user_msg = (
        f"Sender context: {sender_ctx}\n"
        f"Regex pattern score: {scores['regex']:.3f}\n"
        f"BERT classifier (PG2) score: {scores['promptguard']:.3f}\n"
        f"Supervised classifier block-probability: {scores['classifier_a']:.3f}\n\n"
        "Final risk score (0.0-1.0):"
    )
    body = json.dumps({
        "model": model,
        "temperature": 0,
        "max_tokens": 8,
        "messages": [
            {"role": "system", "content": LLM_SYSTEM_PROMPT},
            {"role": "user", "content": user_msg},
        ],
    }).encode()
    req = urllib.request.Request(  # nosec B310
        endpoint,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:  # nosec B310
        content = json.loads(resp.read())["choices"][0]["message"]["content"].strip()
    try:
        val = float(content.split()[0].rstrip(".,"))
        return max(0.0, min(1.0, val))
    except (ValueError, IndexError):
        return None


# ── tiered decision ───────────────────────────────────────────────────────────


def tiered_decision(
    msg_id: str,
    context: dict[str, Any],
    config: dict[str, Any],
) -> tuple[DecisionPayload, list[LogEntry]]:
    trust_level = _str_from(context, "sender", "trust_level", default=TRUST_UNKNOWN)
    scores = read_scores(context)
    logs: list[LogEntry] = []

    llm_endpoint = str(config.get("llm_endpoint_url") or DEFAULT_LLM_ENDPOINT)
    llm_model = str(config.get("llm_model") or DEFAULT_LLM_MODEL)
    llm_timeout = int(config.get("llm_timeout_s") or DEFAULT_LLM_TIMEOUT_S)

    max_score = max(scores.values())
    all_low = all(v <= FAST_PASS_THRESHOLD for v in scores.values())

    # Fast-pass: all scores comfortably below threshold
    if all_low:
        logs.append({"level": "info", "message": f"fast_pass: all scores ≤ {FAST_PASS_THRESHOLD}"})
        return {
            "message_id": str(msg_id),
            "decision": DECISION_PROCESS,
            "path": "fast_pass",
            "trust_level": trust_level,
            "scores": scores,
            "llm_score": None,
        }, logs

    # Fast-block: any score very high AND sender is untrusted
    if max_score >= FAST_BLOCK_THRESHOLD and trust_level != TRUST_TRUSTED:
        logs.append({
            "level": "info",
            "message": (
                f"fast_block: max_score={max_score:.3f} >= {FAST_BLOCK_THRESHOLD}, sender=unknown"
            ),
        })
        return {
            "message_id": str(msg_id),
            "decision": DECISION_QUARANTINE,
            "path": "fast_block",
            "trust_level": trust_level,
            "scores": scores,
            "llm_score": None,
        }, logs

    # Edge case (or trusted sender with high score) — call LLM
    llm_score: float | None = None
    try:
        llm_score = _llm_adjudicate(
            scores, trust_level, endpoint=llm_endpoint, model=llm_model, timeout_s=llm_timeout
        )
    except (urllib.error.URLError, urllib.error.HTTPError, KeyError, json.JSONDecodeError) as exc:
        logs.append({"level": "warn", "message": f"LLM call failed ({exc}); fallback quarantine"})
        return {
            "message_id": str(msg_id),
            "decision": DECISION_QUARANTINE,
            "path": "llm_fallback",
            "trust_level": trust_level,
            "scores": scores,
            "llm_score": None,
        }, logs

    if llm_score is None:
        logs.append({"level": "warn", "message": "LLM unparseable response; fallback quarantine"})
        return {
            "message_id": str(msg_id),
            "decision": DECISION_QUARANTINE,
            "path": "llm_fallback",
            "trust_level": trust_level,
            "scores": scores,
            "llm_score": None,
        }, logs

    decision = DECISION_QUARANTINE if llm_score >= LLM_BLOCK_THRESHOLD else DECISION_PROCESS
    logs.append({
        "level": "info",
        "message": (
            f"llm_adjudicated: llm_score={llm_score:.3f} → {decision} "
            f"(trust={trust_level}, max_score={max_score:.3f})"
        ),
    })
    return {
        "message_id": str(msg_id),
        "decision": decision,
        "path": "llm_adjudicated",
        "trust_level": trust_level,
        "scores": scores,
        "llm_score": round(llm_score, 4),
    }, logs


# ── handle ────────────────────────────────────────────────────────────────────


def cmd_handle(config: dict[str, Any], context: dict[str, Any]) -> ResponseOk | ResponseErr:
    mail = context.get("mail")
    if not isinstance(mail, dict):
        return err("context.mail is missing or not an object")

    msg_id = mail.get("message_id")
    if not msg_id:
        return err("context.mail.message_id is missing")

    payload, logs = tiered_decision(str(msg_id), context, config)
    summary = (
        f"{payload['decision'].upper()} {msg_id}: "
        f"path={payload['path']}, trust={payload['trust_level']}, "
        f"scores=({payload['scores']['regex']:.3f},{payload['scores']['promptguard']:.3f},"
        f"{payload['scores']['classifier_a']:.3f})"
    )
    if payload.get("llm_score") is not None:
        summary += f", llm={payload['llm_score']:.3f}"

    logs.insert(0, {"level": "info", "message": summary})
    return ok(
        summary,
        logs=logs,
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
    endpoint = str(config.get("llm_endpoint_url") or DEFAULT_LLM_ENDPOINT)
    model = str(config.get("llm_model") or DEFAULT_LLM_MODEL)
    msg = (
        f"email_pipeline_veto healthy — 4-judge tiered fusion "
        f"(fast_pass≤{FAST_PASS_THRESHOLD}, fast_block≥{FAST_BLOCK_THRESHOLD}), "
        f"llm={model}@{endpoint}"
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
