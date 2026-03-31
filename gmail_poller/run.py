#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""gmail_poller — Ductile plugin (protocol v2).

Polls Gmail for new messages via Gmail's History API (historyId) and emits
gmail.new_message signal events. Auth is delegated entirely to the gws CLI;
no credentials are handled in this plugin.

State keys managed:
  last_history_id     (str)  — Gmail historyId from last successful poll
  last_poll_at        (str)  — ISO-8601 timestamp of last poll
  history_reset_count (int)  — cumulative count of historyId gap resets

Event emitted: gmail.new_message
  payload.message_id  — Gmail message ID (use with gws to fetch full content)
  payload.thread_id   — Gmail thread ID
  payload.from        — From header
  payload.subject     — Subject header
  payload.snippet     — Gmail-generated plain-text snippet (~100 chars)
  payload.label_ids   — Label IDs at time of detection
  payload.received_at — Date header value

Config keys (all optional):
  gws_binary        (str,  default: "gws")   — path/name of gws executable
  label_filter      (str,  default: "INBOX") — Gmail label ID to watch
  max_per_poll      (int,  default: 20)      — max events emitted per tick
  emit_on_first_run (bool, default: false)   — reserved; first run always skips
"""
from __future__ import annotations

import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from typing import Any


# ── response helpers ──────────────────────────────────────────────────────────

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ok(
    result: str,
    logs: list[dict] | None = None,
    events: list[dict] | None = None,
    state_updates: dict | None = None,
) -> dict[str, Any]:
    resp: dict[str, Any] = {
        "status": "ok",
        "result": result,
        "logs": logs or [{"level": "info", "message": result}],
    }
    if events:
        resp["events"] = events
    if state_updates is not None:
        resp["state_updates"] = state_updates
    return resp


def err(message: str, *, retry: bool = False, logs: list[dict] | None = None) -> dict[str, Any]:
    return {
        "status": "error",
        "error": message,
        "retry": retry,
        "logs": logs or [{"level": "error", "message": message}],
    }


# ── gws subprocess wrapper ────────────────────────────────────────────────────

class GWSError(Exception):
    def __init__(self, message: str, *, retry: bool = False):
        super().__init__(message)
        self.retry = retry


def gws_run(binary: str, *args: str, timeout: int = 25) -> dict[str, Any]:
    """Invoke gws and return parsed JSON. Raises GWSError on any failure."""
    try:
        result = subprocess.run(
            [binary, *args],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except FileNotFoundError:
        raise GWSError(f"gws binary not found: {binary!r}", retry=False)
    except subprocess.TimeoutExpired:
        raise GWSError(f"gws timed out after {timeout}s", retry=True)

    # gws writes keyring notices to stderr — ignore them; parse stdout only
    stdout = result.stdout.strip()
    if not stdout:
        raise GWSError(
            f"gws returned empty output (exit {result.returncode})", retry=True
        )

    try:
        data = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise GWSError(f"gws output is not valid JSON: {exc}", retry=False)

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

def cmd_health(config: dict) -> dict:
    binary = str(config.get("gws_binary") or "gws")

    if not shutil.which(binary):
        return err(f"gws binary not found in PATH: {binary!r}")

    try:
        profile = gws_run(
            binary, "gmail", "users", "getProfile",
            "--params", '{"userId":"me"}',
            timeout=10,
        )
    except GWSError as exc:
        return err(f"Gmail health check failed: {exc}", retry=exc.retry)

    email = profile.get("emailAddress", "unknown")
    msg = f"gmail_poller healthy — authenticated as {email}"
    return ok(msg, logs=[{"level": "info", "message": msg}])


# ── poll helpers ──────────────────────────────────────────────────────────────

def get_current_history_id(binary: str) -> str:
    """Return the mailbox's current historyId."""
    profile = gws_run(
        binary, "gmail", "users", "getProfile",
        "--params", '{"userId":"me"}',
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
) -> tuple[list[dict], str]:
    """
    Fetch message additions since start_history_id.

    Returns (messages, latest_history_id).
    Raises GWSError with 'history_gap' prefix when historyId is stale (404).
    """
    params = json.dumps({
        "userId": "me",
        "startHistoryId": start_history_id,
        "historyTypes": "messageAdded",
        "labelId": label_filter,
        "maxResults": min(max_results, 500),
    })
    try:
        data = gws_run(
            binary, "gmail", "users", "history", "list",
            "--params", params,
            timeout=20,
        )
    except GWSError as exc:
        if "404" in str(exc):
            raise GWSError(
                f"history_gap: historyId {start_history_id!r} is stale (purged by Gmail)",
                retry=False,
            )
        raise

    latest_id = str(data.get("historyId") or start_history_id)

    # Flatten history entries → unique message stubs
    messages: list[dict] = []
    seen: set[str] = set()
    for entry in data.get("history") or []:
        for added in entry.get("messagesAdded") or []:
            msg = added.get("message") or {}
            msg_id = msg.get("id")
            if msg_id and msg_id not in seen:
                seen.add(msg_id)
                messages.append(msg)

    return messages, latest_id


def fetch_message_meta(binary: str, message_id: str) -> dict[str, Any]:
    """
    Fetch message metadata (headers only, no body).
    Returns dict: from, subject, snippet, label_ids, received_at.
    """
    params = json.dumps({
        "userId": "me",
        "id": message_id,
        "format": "metadata",
        "metadataHeaders": ["From", "Subject", "Date"],
    })
    data = gws_run(
        binary, "gmail", "users", "messages", "get",
        "--params", params,
        timeout=15,
    )
    headers = {
        h["name"]: h["value"]
        for h in data.get("payload", {}).get("headers", [])
    }
    return {
        "from":        headers.get("From", ""),
        "subject":     headers.get("Subject", ""),
        "snippet":     data.get("snippet", ""),
        "label_ids":   data.get("labelIds", []),
        "received_at": headers.get("Date", ""),
    }


# ── poll ──────────────────────────────────────────────────────────────────────

def cmd_poll(config: dict, state: dict) -> dict:
    binary            = str(config.get("gws_binary") or "gws")
    label_filter      = str(config.get("label_filter") or "INBOX")
    max_per_poll      = int(config.get("max_per_poll") or 20)

    last_history_id   = state.get("last_history_id")
    history_reset_count = int(state.get("history_reset_count") or 0)
    logs: list[dict]  = []

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
                "last_history_id":    current_id,
                "last_poll_at":       now_iso(),
                "history_reset_count": 0,
            },
        )

    # ── subsequent runs: fetch history since last tick ────────────────────────
    history_gap = False
    messages: list[dict] = []
    new_history_id = last_history_id

    try:
        messages, new_history_id = fetch_history(
            binary, last_history_id, label_filter, max_per_poll
        )
    except GWSError as exc:
        if str(exc).startswith("history_gap:"):
            history_gap = True
            logs.append({
                "level": "warn",
                "message": (
                    f"historyId gap: ID {last_history_id!r} is too old (Gmail purged history). "
                    "Resetting to current. Some messages between last poll and now may be missed."
                ),
            })
            try:
                new_history_id = get_current_history_id(binary)
            except GWSError as inner:
                return err(
                    f"Failed to reset historyId after gap: {inner}",
                    retry=inner.retry,
                    logs=logs,
                )
        else:
            return err(str(exc), retry=exc.retry)

    state_updates = {
        "last_history_id":    new_history_id,
        "last_poll_at":       now_iso(),
        "history_reset_count": history_reset_count + (1 if history_gap else 0),
    }

    if history_gap or not messages:
        summary = "historyId reset — skipping this tick." if history_gap else "No new messages."
        logs.append({"level": "info", "message": summary})
        return ok(summary, logs=logs, state_updates=state_updates)

    # ── build events: fetch metadata for each new message ────────────────────
    events: list[dict] = []
    meta_failures = 0

    for msg in messages[:max_per_poll]:
        msg_id = msg.get("id")
        if not msg_id:
            continue
        try:
            meta = fetch_message_meta(binary, msg_id)
        except GWSError as exc:
            meta_failures += 1
            logs.append({
                "level": "warn",
                "message": f"Skipped metadata fetch for {msg_id}: {exc}",
            })
            continue

        events.append({
            "type": "gmail.new_message",
            "payload": {
                "message_id":  msg_id,
                "thread_id":   msg.get("threadId", ""),
                "from":        meta["from"],
                "subject":     meta["subject"],
                "snippet":     meta["snippet"],
                "label_ids":   meta["label_ids"],
                "received_at": meta["received_at"],
            },
            "dedupe_key": f"gmail:msg:{msg_id}",
        })

    summary = f"Emitted {len(events)} new message(s)."
    if meta_failures:
        summary += f" {meta_failures} metadata fetch(es) failed (see logs)."
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
    config  = req.get("config")  or {}
    state   = req.get("state")   or {}

    if command == "poll":
        out = cmd_poll(config, state)
    elif command == "health":
        out = cmd_health(config)
    else:
        out = err(f"Unknown command: {command!r}")

    json.dump(out, sys.stdout)
    sys.stdout.write("\n")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
