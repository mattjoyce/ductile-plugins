#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""email_quarantine_label — Ductile plugin (protocol v2).

Act-step for the email security pipeline. When the pipeline reaches a
QUARANTINE decision, this plugin applies a Gmail label (default
"ductile/needs-review") to the message and leaves it in place — making
quarantined mail visible in the inbox the operator already lives in,
rather than silently dropped.

This plugin does NO scoring. The decision is made upstream (email_pipeline_veto
emits email.process_decision); this is the separate act so the scorer stays
a pure value-producer (decision ⊥ action).

Event emitted: email.quarantine_labeled
  payload.message_id  — Gmail message ID labeled
  payload.label_name  — label applied
  payload.label_id    — resolved Gmail label ID
  dedupe_key          — quarantine-label:msg:<message_id>

Config keys (all optional):
  gws_binary  (str)  — gws CLI path (default "gws")
  label_name  (str)  — label to apply (default "ductile/needs-review")
"""

from __future__ import annotations

import json
import shutil
import subprocess  # nosec B404
import sys
from typing import Any, NotRequired, TypedDict

EVENT_TYPE = "email.quarantine_labeled"
DEDUPE_PREFIX = "quarantine-label:msg:"
CMD_HANDLE = "handle"
CMD_HEALTH = "health"

DECISION_QUARANTINE = "quarantine"
DEFAULT_LABEL_NAME = "ductile/needs-review"


# ── protocol shapes ───────────────────────────────────────────────────────────


class LogEntry(TypedDict):
    level: str
    message: str


class ResponseOk(TypedDict):
    status: str
    result: str
    logs: list[LogEntry]
    events: NotRequired[list[dict[str, Any]]]


class ResponseErr(TypedDict):
    status: str
    error: str
    retry: bool
    logs: list[LogEntry]


def ok(
    result: str,
    *,
    logs: list[LogEntry] | None = None,
    events: list[dict[str, Any]] | None = None,
) -> ResponseOk:
    resp: ResponseOk = {
        "status": "ok",
        "result": result,
        "logs": logs or [{"level": "info", "message": result}],
    }
    if events:
        resp["events"] = events
    return resp


def err(message: str, *, retry: bool = False, logs: list[LogEntry] | None = None) -> ResponseErr:
    return {
        "status": "error",
        "error": message,
        "retry": retry,
        "logs": logs or [{"level": "error", "message": message}],
    }


# ── gws subprocess wrapper (convention copied from email_pipeline_fetch) ─────────


class GWSError(Exception):
    def __init__(self, message: str, *, retry: bool = False) -> None:
        super().__init__(message)
        self.retry = retry


def gws_run(binary: str, *args: str, timeout: int = 25) -> dict[str, Any]:
    """Invoke gws and return parsed JSON. Raises GWSError on any failure.

    args are literal command tokens (binary path + plugin-author gws subcommand
    strings); shell=False; no user-supplied input reaches argv (B603/B404).
    """
    try:
        result = subprocess.run(  # nosec B603
            [binary, *args],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except FileNotFoundError as exc:
        raise GWSError(f"gws binary not found: {binary!r}", retry=False) from exc
    except subprocess.TimeoutExpired as exc:
        raise GWSError(f"gws timed out after {timeout}s", retry=True) from exc

    stdout = result.stdout.strip()
    if not stdout:
        raise GWSError(f"gws returned empty output (exit {result.returncode})", retry=True)

    try:
        data = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise GWSError(f"gws output is not valid JSON: {exc}", retry=False) from exc

    if "error" in data:
        api_err = data["error"]
        code = api_err.get("code", 0)
        message = api_err.get("message", str(api_err))
        if code in (401, 403):
            raise GWSError(f"Gmail auth/permission error ({code}): {message}", retry=False)
        if code == 404:
            raise GWSError(f"Gmail 404: {message}", retry=False)
        if code == 429 or code >= 500:
            raise GWSError(f"Gmail transient error ({code}): {message}", retry=True)
        raise GWSError(f"Gmail API error ({code}): {message}", retry=False)

    return data


# ── label resolution ────────────────────────────────────────────────────────────


def resolve_label_id(binary: str, label_name: str) -> str:
    """Return the Gmail label ID for label_name, creating the label if absent."""
    listing = gws_run(binary, "gmail", "users", "labels", "list", "--params", '{"userId":"me"}')
    for lbl in listing.get("labels", []):
        if lbl.get("name") == label_name:
            return str(lbl["id"])

    created = gws_run(
        binary,
        "gmail",
        "users",
        "labels",
        "create",
        "--params",
        '{"userId":"me"}',
        "--json",
        json.dumps(
            {
                "name": label_name,
                "labelListVisibility": "labelShow",
                "messageListVisibility": "show",
            }
        ),
    )
    label_id = created.get("id")
    if not label_id:
        raise GWSError(f"label create returned no id for {label_name!r}")
    return str(label_id)


# ── handle ───────────────────────────────────────────────────────────────────────


def _read_decision(context: dict[str, Any]) -> str | None:
    """Best-effort read of the upstream decision, for a defensive no-op guard."""
    for path in (("decision",), ("payload", "decision")):
        cur: Any = context
        for k in path:
            if not isinstance(cur, dict):
                cur = None
                break
            cur = cur.get(k)
        if isinstance(cur, str):
            return cur
    return None


def cmd_handle(config: dict[str, Any], context: dict[str, Any]) -> ResponseOk | ResponseErr:
    binary = str(config.get("gws_binary") or "gws")
    label_name = str(config.get("label_name") or DEFAULT_LABEL_NAME)

    # Defensive: this plugin should be routed only for quarantine decisions, but
    # if a decision is present and is not quarantine, no-op rather than mislabel.
    decision = _read_decision(context)
    if decision is not None and decision != DECISION_QUARANTINE:
        msg = f"decision={decision!r} is not quarantine; skipped"
        return ok(msg, logs=[{"level": "info", "message": msg}])

    mail = context.get("mail")
    if not isinstance(mail, dict):
        return err("context.mail is missing or not an object")
    msg_id = mail.get("message_id")
    if not msg_id:
        return err("context.mail.message_id is missing")

    try:
        label_id = resolve_label_id(binary, label_name)
        gws_run(
            binary,
            "gmail",
            "users",
            "messages",
            "modify",
            "--params",
            json.dumps({"userId": "me", "id": str(msg_id)}),
            "--json",
            json.dumps({"addLabelIds": [label_id]}),
        )
    except GWSError as exc:
        return err(f"labeling failed for {msg_id}: {exc}", retry=exc.retry)

    summary = f"labeled {msg_id} → {label_name}"
    return ok(
        summary,
        logs=[{"level": "info", "message": summary}],
        events=[
            {
                "type": EVENT_TYPE,
                "payload": {
                    "message_id": str(msg_id),
                    "label_name": label_name,
                    "label_id": label_id,
                },
                "dedupe_key": f"{DEDUPE_PREFIX}{msg_id}",
            }
        ],
    )


# ── health ───────────────────────────────────────────────────────────────────────


def cmd_health(config: dict[str, Any]) -> ResponseOk | ResponseErr:
    binary = str(config.get("gws_binary") or "gws")
    label_name = str(config.get("label_name") or DEFAULT_LABEL_NAME)
    if not shutil.which(binary):
        return err(f"gws binary not found in PATH: {binary!r}")
    msg = f"email_quarantine_label healthy — gws={binary}, label={label_name!r}"
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
