#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""email_pipeline_fetch — Ductile plugin (protocol v2).

Polls Gmail for new messages via the History API (historyId) and emits one
gmail.new_full_message event per new message, carrying the complete
unmodified Gmail Message JSON (format=full) inline on the event payload.

State keys managed (durable via manifest fact_outputs):
  last_history_id     (str)  — Gmail historyId from last successful poll
  last_poll_at        (str)  — ISO-8601 timestamp of last poll
  history_reset_count (int)  — cumulative count of historyId gap resets

Event emitted: gmail.new_full_message
  payload.message_id        — Gmail message ID
  payload.thread_id         — Gmail thread ID
  payload.raw_message_json  — full Gmail Message resource JSON (format=full)
  dedupe_key                — gmail-full:msg:<message_id>
"""

from __future__ import annotations

import json
import shutil
import subprocess  # nosec B404
import sys
from datetime import UTC, datetime
from typing import Any, NotRequired, TypedDict

EVENT_TYPE = "gmail.new_full_message"
DEDUPE_PREFIX = "gmail-full:msg:"
GMAIL_HISTORY_PAGE_CAP = 500
CMD_POLL = "poll"
CMD_HEALTH = "health"


# ── protocol shapes ───────────────────────────────────────────────────────────


class LogEntry(TypedDict):
    level: str
    message: str


class StateSnapshot(TypedDict):
    last_history_id: str
    last_poll_at: str
    history_reset_count: int


class FetchEventPayload(TypedDict):
    message_id: str
    thread_id: str
    raw_message_json: dict[str, Any]


class FetchEvent(TypedDict):
    type: str
    payload: FetchEventPayload
    dedupe_key: str


class ResponseOk(TypedDict):
    status: str
    result: str
    logs: list[LogEntry]
    events: NotRequired[list[FetchEvent]]
    state_updates: NotRequired[StateSnapshot]


class ResponseErr(TypedDict):
    status: str
    error: str
    retry: bool
    logs: list[LogEntry]


# ── response helpers ──────────────────────────────────────────────────────────


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


def ok(
    result: str,
    *,
    logs: list[LogEntry] | None = None,
    events: list[FetchEvent] | None = None,
    state_updates: StateSnapshot | None = None,
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


# ── gws subprocess wrapper ────────────────────────────────────────────────────


class GWSError(Exception):
    def __init__(self, message: str, *, retry: bool = False) -> None:
        super().__init__(message)
        self.retry = retry


class HistoryGapError(GWSError):
    """Gmail purged the historyId we were tracking; reset to current."""


def gws_run(binary: str, *args: str, timeout: int = 25) -> dict[str, Any]:
    """Invoke gws and return parsed JSON. Raises GWSError on any failure.

    Justification for bandit B603/B404 suppression: args are literal command
    tokens (binary path, then plugin-author-provided gws subcommand strings);
    shell=False; no user-supplied input reaches the argv. Standard exception
    for CLI-wrapping plugins.
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

    # gws writes keyring notices to stderr — ignore them; parse stdout only
    stdout = result.stdout.strip()
    if not stdout:
        raise GWSError(f"gws returned empty output (exit {result.returncode})", retry=True)

    try:
        data = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise GWSError(f"gws output is not valid JSON: {exc}", retry=False) from exc

    # gws embeds API errors in the JSON body rather than using exit codes
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


# ── health ────────────────────────────────────────────────────────────────────


def cmd_health(config: dict[str, Any]) -> ResponseOk | ResponseErr:
    binary = str(config.get("gws_binary") or "gws")

    if not shutil.which(binary):
        return err(f"gws binary not found in PATH: {binary!r}")

    try:
        profile = gws_run(
            binary,
            "gmail",
            "users",
            "getProfile",
            "--params",
            '{"userId":"me"}',
            timeout=10,
        )
    except GWSError as exc:
        return err(f"Gmail health check failed: {exc}", retry=exc.retry)

    email = profile.get("emailAddress", "unknown")
    msg = f"email_pipeline_fetch healthy — authenticated as {email}"
    return ok(msg, logs=[{"level": "info", "message": msg}])


# ── poll helpers ──────────────────────────────────────────────────────────────


def get_current_history_id(binary: str) -> str:
    """Return the mailbox's current historyId."""
    profile = gws_run(
        binary,
        "gmail",
        "users",
        "getProfile",
        "--params",
        '{"userId":"me"}',
        timeout=15,
    )
    history_id = profile.get("historyId")
    if not history_id:
        raise GWSError("Profile response missing historyId", retry=True)
    return str(history_id)


def fetch_history(
    binary: str,
    start_history_id: str,
    label_filter: str,
    max_results: int,
) -> tuple[list[dict[str, Any]], str]:
    """Fetch message additions since start_history_id.

    Returns (messages, latest_history_id).
    Raises HistoryGapError when historyId is stale (404).
    """
    params = json.dumps(
        {
            "userId": "me",
            "startHistoryId": start_history_id,
            "historyTypes": "messageAdded",
            "labelId": label_filter,
            "maxResults": min(max_results, GMAIL_HISTORY_PAGE_CAP),
        }
    )
    try:
        data = gws_run(
            binary,
            "gmail",
            "users",
            "history",
            "list",
            "--params",
            params,
            timeout=20,
        )
    except GWSError as exc:
        if "404" in str(exc):
            raise HistoryGapError(
                f"historyId {start_history_id!r} is stale (purged by Gmail)",
                retry=False,
            ) from exc
        raise

    latest_id = str(data.get("historyId") or start_history_id)

    messages: list[dict[str, Any]] = []
    seen: set[str] = set()
    for entry in data.get("history") or []:
        for added in entry.get("messagesAdded") or []:
            msg = added.get("message") or {}
            msg_id = msg.get("id")
            if msg_id and msg_id not in seen:
                seen.add(msg_id)
                messages.append(msg)

    return messages, latest_id


def fetch_full_message(binary: str, message_id: str) -> dict[str, Any]:
    """Fetch the complete Gmail Message resource (format=full).

    Returns the unmodified parsed JSON body — payload tree, headers, MIME parts,
    label IDs, snippet, threadId, internal date, etc. The downstream sanitiser
    parses this directly; we do not interpret or decode any of it here.
    """
    params = json.dumps(
        {
            "userId": "me",
            "id": message_id,
            "format": "full",
        }
    )
    return gws_run(
        binary,
        "gmail",
        "users",
        "messages",
        "get",
        "--params",
        params,
        timeout=30,
    )


# ── poll ──────────────────────────────────────────────────────────────────────


def cmd_poll(config: dict[str, Any], state: dict[str, Any]) -> ResponseOk | ResponseErr:
    binary = str(config.get("gws_binary") or "gws")
    label_filter = str(config.get("label_filter") or "INBOX")
    max_per_poll = int(config.get("max_per_poll") or 20)

    last_history_id = state.get("last_history_id")
    history_reset_count = int(state.get("history_reset_count") or 0)
    logs: list[LogEntry] = []

    # ── first run: establish baseline, emit nothing ───────────────────────────
    if not last_history_id:
        try:
            current_id = get_current_history_id(binary)
        except GWSError as exc:
            return err(f"Failed to initialise historyId: {exc}", retry=exc.retry)

        msg = f"First run — baseline historyId={current_id}. No messages emitted."
        logs.append({"level": "info", "message": msg})
        return ok(
            "First run: baseline historyId recorded.",
            logs=logs,
            state_updates={
                "last_history_id": current_id,
                "last_poll_at": now_iso(),
                "history_reset_count": 0,
            },
        )

    # ── subsequent runs: fetch history since last tick ────────────────────────
    history_gap = False
    messages: list[dict[str, Any]] = []
    new_history_id = last_history_id

    try:
        messages, new_history_id = fetch_history(
            binary, last_history_id, label_filter, max_per_poll
        )
    except HistoryGapError:
        history_gap = True
        logs.append(
            {
                "level": "warn",
                "message": (
                    f"historyId gap: ID {last_history_id!r} is too old (Gmail purged history). "
                    "Resetting to current. Some messages between last poll and now may be missed."
                ),
            }
        )
        try:
            new_history_id = get_current_history_id(binary)
        except GWSError as inner:
            return err(
                f"Failed to reset historyId after gap: {inner}",
                retry=inner.retry,
                logs=logs,
            )
    except GWSError as exc:
        return err(str(exc), retry=exc.retry)

    state_updates: StateSnapshot = {
        "last_history_id": new_history_id,
        "last_poll_at": now_iso(),
        "history_reset_count": history_reset_count + (1 if history_gap else 0),
    }

    if history_gap or not messages:
        summary = "historyId reset — skipping this tick." if history_gap else "No new messages."
        logs.append({"level": "info", "message": summary})
        return ok(summary, logs=logs, state_updates=state_updates)

    # ── build events: fetch full message JSON for each new message ───────────
    events: list[FetchEvent] = []
    fetch_failures = 0

    for msg in messages[:max_per_poll]:
        msg_id = msg.get("id")
        if not msg_id:
            continue
        try:
            full = fetch_full_message(binary, msg_id)
        except GWSError as exc:
            fetch_failures += 1
            logs.append(
                {
                    "level": "warn",
                    "message": f"Skipped full fetch for {msg_id}: {exc}",
                }
            )
            continue

        events.append(
            {
                "type": EVENT_TYPE,
                "payload": {
                    "message_id": msg_id,
                    "thread_id": msg.get("threadId", ""),
                    "raw_message_json": full,
                },
                "dedupe_key": f"{DEDUPE_PREFIX}{msg_id}",
            }
        )

    summary = f"Emitted {len(events)} {EVENT_TYPE} event(s)."
    if fetch_failures:
        summary += f" {fetch_failures} full-fetch(es) failed (see logs)."
    logs.append({"level": "info", "message": summary})

    return ok(summary, logs=logs, events=events, state_updates=state_updates)


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
