#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""email_pipeline_sanitise — Ductile plugin (protocol v2).

Thin Python shim around the standalone Go sanitiser binary. Consumes
gmail.new_full_message events, pipes the Gmail Message JSON through the
binary, emits one email.sanitised event per message with the parsed
facts.Facts payload.

Hard-fail discipline: any error from the sanitiser binary, missing input
fields, JSON parse failure, or schema-version mismatch returns
status: error with retry: false. Subprocess timeout is the one retryable
error path.

Event consumed: gmail.new_full_message
  event.payload.message_id        — Gmail message ID
  event.payload.raw_message_json  — full Gmail Message resource JSON

Event emitted: email.sanitised
  payload.schema_version         — sanitiser schema version
  payload.message_id             — Gmail Message-ID header (or fallback)
  payload.parts                  — list of {mime_type, sanitised_text, ...}
  payload.mime_summary           — multipart structure summary
  payload.attachments            — list of attachment metadata
  payload.unicode_normalisation  — "nfkc" or empty
  dedupe_key                     — email-sanitised:msg:<message_id>

Config keys (all optional):
  sanitise_binary          (str, default: "sanitise")  — path/name of binary
  expected_schema_version  (str, default: "0.1.0")     — required output schema
"""

from __future__ import annotations

import json
import subprocess  # nosec B404
import sys
from typing import Any, NotRequired, TypedDict

EVENT_TYPE = "email.sanitised"
DEDUPE_PREFIX = "email-sanitised:msg:"
DEFAULT_BINARY = "sanitise"
DEFAULT_SCHEMA_VERSION = "0.1.0"
HANDLE_TIMEOUT_SECONDS = 25
HEALTH_TIMEOUT_SECONDS = 5
CMD_HANDLE = "handle"
CMD_HEALTH = "health"


# ── protocol shapes ───────────────────────────────────────────────────────────


class LogEntry(TypedDict):
    level: str
    message: str


class FactsPart(TypedDict):
    mime_type: str
    sanitised_text: str
    transfer_encoding: NotRequired[str]
    byte_count: int
    facts: NotRequired[list[str] | None]


class FactsMimeSummary(TypedDict):
    top_level_type: NotRequired[str]
    part_count: int
    has_plain_text: bool
    has_html: bool
    has_calendar: bool


class FactsAttachment(TypedDict):
    mime_type: str
    filename: NotRequired[str]
    size_bytes: NotRequired[int]


class Facts(TypedDict):
    schema_version: str
    message_id: str
    parts: NotRequired[list[FactsPart]]
    mime_summary: FactsMimeSummary
    attachments: NotRequired[list[FactsAttachment]]
    unicode_normalisation: NotRequired[str]


class SanitiseEvent(TypedDict):
    type: str
    payload: Facts
    dedupe_key: str


class ResponseOk(TypedDict):
    status: str
    result: str
    logs: list[LogEntry]
    events: NotRequired[list[SanitiseEvent]]


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
    events: list[SanitiseEvent] | None = None,
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


# ── sanitiser subprocess wrapper ──────────────────────────────────────────────


class SanitiserError(Exception):
    def __init__(self, message: str, *, retry: bool = False) -> None:
        super().__init__(message)
        self.retry = retry


def run_sanitise(binary: str, gmail_json: bytes, *, timeout: int) -> dict[str, Any]:
    """Invoke the sanitise binary with gmail_json on stdin.

    Returns the parsed Facts JSON on success. Raises SanitiserError on any failure.

    Justification for bandit B603 suppression: binary is plugin-config-supplied
    (not user input); shell=False; argv has no shell metacharacters. Standard
    exception for CLI-wrapping plugins.
    """
    try:
        result = subprocess.run(  # nosec B603
            [binary],
            input=gmail_json,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
    except FileNotFoundError as exc:
        raise SanitiserError(f"sanitise binary not found: {binary!r}", retry=False) from exc
    except subprocess.TimeoutExpired as exc:
        raise SanitiserError(f"sanitise timed out after {timeout}s", retry=True) from exc

    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        raise SanitiserError(
            f"sanitise exited {result.returncode}: {stderr or '<no stderr>'}",
            retry=False,
        )

    stdout = result.stdout
    if not stdout.strip():
        raise SanitiserError("sanitise produced empty stdout", retry=False)

    try:
        return json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise SanitiserError(f"sanitise output is not valid JSON: {exc}", retry=False) from exc


# ── health ────────────────────────────────────────────────────────────────────


def cmd_health(config: dict[str, Any]) -> ResponseOk | ResponseErr:
    """Verify the sanitise binary is reachable and reports the expected schema.

    Justification for bandit B603 suppression on the subprocess.run call below:
    binary is plugin-config-supplied; shell=False; argv has no metacharacters.
    Same pattern as run_sanitise.
    """
    binary = str(config.get("sanitise_binary") or DEFAULT_BINARY)
    expected = str(config.get("expected_schema_version") or DEFAULT_SCHEMA_VERSION)

    try:
        result = subprocess.run(  # nosec B603
            [binary, "--version"],
            capture_output=True,
            timeout=HEALTH_TIMEOUT_SECONDS,
            check=False,
        )
    except FileNotFoundError:
        return err(f"sanitise binary not found: {binary!r}")
    except subprocess.TimeoutExpired:
        return err(f"sanitise --version timed out after {HEALTH_TIMEOUT_SECONDS}s", retry=True)

    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        return err(f"sanitise --version exited {result.returncode}: {stderr or '<no stderr>'}")

    actual = result.stdout.decode("utf-8", errors="replace").strip()
    if actual != expected:
        return err(f"sanitise schema version mismatch: got {actual!r}, expected {expected!r}")

    msg = f"email_pipeline_sanitise healthy — binary {binary!r} reports schema version {actual}"
    return ok(msg)


# ── handle ────────────────────────────────────────────────────────────────────


def cmd_handle(config: dict[str, Any], event: dict[str, Any]) -> ResponseOk | ResponseErr:
    binary = str(config.get("sanitise_binary") or DEFAULT_BINARY)
    expected = str(config.get("expected_schema_version") or DEFAULT_SCHEMA_VERSION)

    payload = event.get("payload", {}) if isinstance(event, dict) else {}
    if not isinstance(payload, dict):
        payload = {}

    msg_id = payload.get("message_id")
    if not msg_id:
        return err("event.payload.message_id is missing")

    raw_obj = payload.get("raw_message_json")
    if raw_obj is None:
        return err("event.payload.raw_message_json is missing")

    try:
        gmail_bytes = json.dumps(raw_obj, separators=(",", ":")).encode("utf-8")
    except (TypeError, ValueError) as exc:
        return err(f"event.payload.raw_message_json is not JSON-serialisable: {exc}")

    try:
        facts = run_sanitise(binary, gmail_bytes, timeout=HANDLE_TIMEOUT_SECONDS)
    except SanitiserError as exc:
        return err(str(exc), retry=exc.retry)

    actual_schema = str(facts.get("schema_version", ""))
    if actual_schema != expected:
        return err(
            f"sanitise output schema version mismatch: got {actual_schema!r}, expected {expected!r}"
        )

    if not facts.get("message_id"):
        facts["message_id"] = msg_id

    event_out: SanitiseEvent = {
        "type": EVENT_TYPE,
        "payload": facts,
        "dedupe_key": f"{DEDUPE_PREFIX}{msg_id}",
    }

    parts_count = len(facts.get("parts", []) or [])
    summary = f"Sanitised {msg_id}: {parts_count} text part(s), schema {actual_schema}."
    return ok(summary, events=[event_out])


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
