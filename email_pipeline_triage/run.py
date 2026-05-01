#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""email_pipeline_triage — Ductile plugin (protocol v2).

First stage of the ductile email pipeline. Extracts sender identity from the
raw Gmail Message JSON and determines trust level from a single signal: whether
the message carries the configured trusted-sender Gmail label.

Trust is managed entirely via Gmail labels/filters. Apply the label manually or
via a Gmail filter; this plugin reads it from the message labelIds list.

Safe default: any parsing failure → trust_level = "unknown". Never blocks.

Event consumed: gmail.new_full_message (first step — reads from event, not context)
  event.payload.message_id        — Gmail message ID
  event.payload.raw_message_json  — full Gmail Message resource JSON

Event emitted: email.triaged
  payload.message_id   — pass-through
  payload.address      — sender email address (lower-cased; empty string if unparseable)
  payload.trust_level  — "trusted" | "unknown"
  dedupe_key           — email-triaged:msg:<message_id>

Config keys (all optional):
  trusted_label_id  (str)  — Gmail label ID for ductile/trusted-sender (default "Label_1")
"""

from __future__ import annotations

import json
import re
import sys
from typing import Any, NotRequired, TypedDict

EVENT_TYPE = "email.triaged"
DEDUPE_PREFIX = "email-triaged:msg:"
DEFAULT_TRUSTED_LABEL_ID = "Label_1"
CMD_HANDLE = "handle"
CMD_HEALTH = "health"

TRUST_TRUSTED = "trusted"
TRUST_UNKNOWN = "unknown"

_EMAIL_RE = re.compile(r"<([^>]+)>")


# ── protocol shapes ───────────────────────────────────────────────────────────


class LogEntry(TypedDict):
    level: str
    message: str


class TriagedPayload(TypedDict):
    message_id: str
    address: str
    trust_level: str


class TriagedEvent(TypedDict):
    type: str
    payload: TriagedPayload
    dedupe_key: str


class ResponseOk(TypedDict):
    status: str
    result: str
    logs: list[LogEntry]
    events: NotRequired[list[TriagedEvent]]


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
    events: list[TriagedEvent] | None = None,
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


# ── sender extraction ─────────────────────────────────────────────────────────


def _extract_sender(raw_message_json: dict[str, Any]) -> str:
    """Return the sender email address from the Gmail Message resource.

    Handles "Name <email>" and bare "email" header values. Returns empty string
    if the From header is absent or unparseable — callers must handle this.
    """
    try:
        headers: list[dict[str, str]] = (
            raw_message_json.get("payload", {}).get("headers") or []
        )
        for hdr in headers:
            if isinstance(hdr, dict) and hdr.get("name", "").lower() == "from":
                value = str(hdr.get("value", "")).strip()
                m = _EMAIL_RE.search(value)
                if m:
                    return m.group(1).strip().lower()
                if "@" in value:
                    return value.lower()
    except Exception:  # nosec B110
        pass
    return ""


# ── trust evaluation ──────────────────────────────────────────────────────────


def _determine_trust(raw_message_json: dict[str, Any], trusted_label_id: str) -> tuple[str, str]:
    """Return (trust_level, reason). Trusted iff message carries the trusted label."""
    try:
        label_ids = raw_message_json.get("labelIds") or []
        if trusted_label_id in label_ids:
            return TRUST_TRUSTED, f"label:{trusted_label_id}"
    except Exception:  # nosec B110
        pass
    return TRUST_UNKNOWN, "no_trust_label"


# ── handle ────────────────────────────────────────────────────────────────────


def cmd_handle(config: dict[str, Any], event: dict[str, Any]) -> ResponseOk | ResponseErr:
    payload = event.get("payload", {}) if isinstance(event, dict) else {}
    if not isinstance(payload, dict):
        payload = {}

    msg_id = payload.get("message_id")
    if not msg_id:
        return err("event.payload.message_id is missing")

    raw_message_json = payload.get("raw_message_json")
    if not isinstance(raw_message_json, dict):
        return err("event.payload.raw_message_json is missing or not an object")

    trusted_label_id = str(config.get("trusted_label_id") or DEFAULT_TRUSTED_LABEL_ID)

    address = _extract_sender(raw_message_json)
    trust_level, reason = _determine_trust(raw_message_json, trusted_label_id)

    out_payload: TriagedPayload = {
        "message_id": str(msg_id),
        "address": address,
        "trust_level": trust_level,
    }
    addr_str = address or "<unknown>"
    summary = f"TRIAGE {msg_id}: address={addr_str} trust={trust_level} reason={reason}"
    return ok(
        summary,
        events=[
            {
                "type": EVENT_TYPE,
                "payload": out_payload,
                "dedupe_key": f"{DEDUPE_PREFIX}{msg_id}",
            }
        ],
    )


# ── health ────────────────────────────────────────────────────────────────────


def cmd_health(config: dict[str, Any]) -> ResponseOk | ResponseErr:
    trusted_label_id = str(config.get("trusted_label_id") or DEFAULT_TRUSTED_LABEL_ID)
    msg = f"email_pipeline_triage healthy — trusted_label_id={trusted_label_id}"
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
    event = req.get("event", {}) if isinstance(req, dict) else {}

    out: ResponseOk | ResponseErr
    if command == CMD_HANDLE:
        out = cmd_handle(config, event)
    elif command == CMD_HEALTH:
        out = cmd_health(config)
    else:
        out = err(f"Unknown command: {command!r}")

    json.dump(out, sys.stdout)
    sys.stdout.write("\n")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
