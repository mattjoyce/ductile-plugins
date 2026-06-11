#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""email_handler — Ductile plugin (protocol v2).

Handles email.process_decision events where decision == "process".
By the time this runs, the email has cleared the upstream security pipeline
(regex + PG2 BERT + Sentinel v2 + Superagent 4B + optional LLM adjudicator).
Trust level, pipeline path, and per-scorer block-probabilities are available
in the event payload.

Steps:
  1. Read upstream pipeline facts (trust_level, path, scores) from event payload
  2. Fetch full message via gws to get From, Subject, body text
  3. Load prompt template from configured path, substitute placeholders
  4. Dispatch to claude -p; claude decides reply / bd task / ignore

The prompt template lives outside this repo (see prompt.example.md for the
shape). Place the customised template at the path given by `prompt_template_path`
config. The plugin requires this config to be set — there is no inline default
prompt to keep personal instructions out of the public repo.

Config keys:
  prompt_template_path      (str, REQUIRED) — path to prompt template file
  gws_binary                (str, default: "gws")
  claude_binary             (str, default: "/Users/mattjoyce/.local/bin/claude")
  claude_working_dir        (str, default: "/Users/mattjoyce/.claude")
  timeout_seconds           (int, default: 300)  — claude -p timeout
  gws_fetch_timeout_seconds (int, default: 30)   — gws fetch timeout
"""

from __future__ import annotations

import base64
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_GWS = "gws"
DEFAULT_CLAUDE = "/Users/mattjoyce/.local/bin/claude"
DEFAULT_CLAUDE_CWD = "/Users/mattjoyce/.claude"
DEFAULT_TIMEOUT = 600
DEFAULT_GWS_FETCH_TIMEOUT = 30
DEFAULT_THREAD_CONTEXT_MESSAGES = 5
BODY_TRUNCATE_CHARS = 6000

REQUIRED_PLACEHOLDERS = (
    "from_addr", "subject", "message_id",
    "trust_level", "pipeline_path", "score_summary", "body",
    "attachments", "mime_summary", "thread_summary",
)


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def plugin_ok(*, result: str, logs: list[dict] | None = None) -> dict[str, Any]:
    return {"status": "ok", "result": result, "logs": logs or []}


def plugin_error(message: str, *, retry: bool = False, logs: list[dict] | None = None) -> dict[str, Any]:
    return {
        "status": "error",
        "error": message,
        "retry": retry,
        "logs": logs or [{"level": "error", "message": message}],
    }


def binary_ok(path: str) -> bool:
    return shutil.which(path) is not None or os.path.isfile(path)


def handle_health(config: dict[str, Any]) -> dict[str, Any]:
    gws = config.get("gws_binary", DEFAULT_GWS)
    claude = config.get("claude_binary", DEFAULT_CLAUDE)
    logs = []
    for name, path in [("gws", gws), ("claude", claude)]:
        if not binary_ok(path):
            return plugin_error(f"{name} binary not found: {path}")
        logs.append({"level": "info", "message": f"{name} found at {path}"})
    return plugin_ok(result="email_handler health check passed", logs=logs)


def _header(headers: list[dict], name: str) -> str:
    for h in headers:
        if isinstance(h, dict) and h.get("name", "").lower() == name.lower():
            return str(h.get("value", "")).strip()
    return ""


def _extract_text(part: dict[str, Any], depth: int = 0) -> str:
    """Recursively extract plain text from a Gmail message part tree."""
    if depth > 10:
        return ""
    mime = str(part.get("mimeType", "")).lower()
    body = part.get("body", {})
    data = body.get("data", "") if isinstance(body, dict) else ""

    if mime == "text/plain" and data:
        try:
            return base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="replace")
        except Exception:  # nosec B110
            pass

    for sub in part.get("parts") or []:
        text = _extract_text(sub, depth + 1)
        if text:
            return text
    return ""


def _walk_parts(part: dict[str, Any], depth: int = 0) -> list[dict[str, Any]]:
    """Walk the Gmail MIME tree and return a flat list of leaf parts.

    Each entry: {mime, filename, size, attachment_id, depth}. Multipart wrappers
    are included with mime starting with "multipart/" so callers can render the
    tree shape; leaf parts are everything else.
    """
    if depth > 10:
        return []
    mime = str(part.get("mimeType", "")).lower()
    body = part.get("body", {}) if isinstance(part.get("body"), dict) else {}
    entry = {
        "mime": mime,
        "filename": str(part.get("filename", "") or ""),
        "size": int(body.get("size", 0) or 0),
        "attachment_id": str(body.get("attachmentId", "") or ""),
        "depth": depth,
    }
    out = [entry]
    for sub in part.get("parts") or []:
        out.extend(_walk_parts(sub, depth + 1))
    return out


def _format_size(n: int) -> str:
    if n <= 0:
        return "0B"
    if n < 1024:
        return f"{n}B"
    if n < 1024 * 1024:
        return f"{n / 1024:.1f}KB"
    return f"{n / (1024 * 1024):.1f}MB"


def _is_attachment(p: dict[str, Any]) -> bool:
    """A part counts as an attachment if it has a filename or non-text/* mime."""
    if p["mime"].startswith("multipart/"):
        return False
    if p["filename"]:
        return True
    if not p["mime"].startswith("text/"):
        return True
    return False


def build_mime_summary(parts: list[dict[str, Any]]) -> str:
    """Produce a one-line MIME tree summary.

    Example: "multipart/alternative[text/plain, text/html] + application/pdf:McKinsey.pdf(214KB)"
    """
    if not parts:
        return "(no parts)"
    root = parts[0]
    if not root["mime"].startswith("multipart/"):
        # single-part message
        if _is_attachment(root):
            label = root["filename"] or "(unnamed)"
            return f"{root['mime']}:{label}({_format_size(root['size'])})"
        return root["mime"]

    leaves_inside_root: list[str] = []
    extras: list[str] = []
    for p in parts[1:]:
        if p["mime"].startswith("multipart/"):
            continue
        if p["depth"] == 1 and not _is_attachment(p):
            leaves_inside_root.append(p["mime"])
        elif _is_attachment(p):
            label = p["filename"] or "(unnamed)"
            extras.append(f"{p['mime']}:{label}({_format_size(p['size'])})")
        else:
            leaves_inside_root.append(p["mime"])
    pieces = [f"{root['mime']}[{', '.join(leaves_inside_root)}]"] if leaves_inside_root else [root["mime"]]
    pieces.extend(extras)
    return " + ".join(pieces)


def build_attachments_block(parts: list[dict[str, Any]]) -> str:
    """List attachment metadata for the prompt.

    One per line: "- filename (mime, size)". Returns "(none)" if there are none.
    """
    rows: list[str] = []
    for p in parts:
        if not _is_attachment(p):
            continue
        label = p["filename"] or "(unnamed)"
        rows.append(f"- {label} ({p['mime']}, {_format_size(p['size'])})")
    return "\n".join(rows) if rows else "(none)"


def fetch_message(
    gws: str, message_id: str, timeout: int
) -> tuple[str, str, str, str, list[dict[str, Any]], str, list[dict]]:
    """Fetch full message via gws.

    Returns (from_addr, subject, snippet, body_text, parts, thread_id, logs).
    body_text gets a truncation marker appended when truncated, so the prompt
    is honest with claude about cut content.
    """
    logs: list[dict] = []
    empty_parts: list[dict[str, Any]] = []
    try:
        params = json.dumps({"userId": "me", "id": message_id, "format": "full"})
        result = subprocess.run(  # nosec B603
            [gws, "gmail", "users", "messages", "get", "--params", params],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if result.returncode != 0:
            msg = f"gws exited {result.returncode}: {result.stderr[:200]}"
            logs.append({"level": "warn", "message": msg})
            return "", "", "", f"[fetch failed: {result.stderr[:100]}]", empty_parts, "", logs

        # gws writes keyring notices to stderr — parse stdout only
        stdout = result.stdout.strip()
        # find first '{' in case of leading noise
        brace = stdout.find("{")
        if brace > 0:
            stdout = stdout[brace:]

        try:
            msg_json = json.loads(stdout)
        except json.JSONDecodeError:
            logs.append({"level": "warn", "message": "gws output is not valid JSON"})
            return "", "", "", "[fetch failed: invalid JSON]", empty_parts, "", logs

        payload_obj = msg_json.get("payload", {}) or {}
        headers = payload_obj.get("headers") or []
        from_addr = _header(headers, "From")
        subject = _header(headers, "Subject")
        snippet = str(msg_json.get("snippet", "")).strip()
        thread_id = str(msg_json.get("threadId", "") or "")
        parts = _walk_parts(payload_obj)
        body_text = _extract_text(payload_obj)
        if not body_text:
            body_text = snippet or "[no body text]"

        full_len = len(body_text)
        truncated = full_len > BODY_TRUNCATE_CHARS
        if truncated:
            cut = body_text[:BODY_TRUNCATE_CHARS]
            remaining = full_len - BODY_TRUNCATE_CHARS
            body_text = (
                f"{cut}\n\n[BODY TRUNCATED — {remaining} more chars not shown; "
                f"use `gws gmail users messages get` with format=full to fetch the rest]"
            )
        logs.append({
            "level": "debug",
            "message": f"fetched message: from={from_addr!r} subject={subject!r}"
                       + (f" [body truncated, {full_len} chars total]" if truncated else "")
                       + f" parts={len(parts)}",
        })
        return from_addr, subject, snippet, body_text, parts, thread_id, logs

    except subprocess.TimeoutExpired:
        logs.append({"level": "warn", "message": f"gws fetch timed out after {timeout}s"})
        return "", "", "", "[fetch timed out]", empty_parts, "", logs


def fetch_thread_summary(
    gws: str,
    thread_id: str,
    current_message_id: str,
    max_messages: int,
    timeout: int,
) -> tuple[str, list[dict]]:
    """Fetch thread metadata and produce a short prior-context summary.

    Returns ("(none)", logs) when there's no prior context (single-message thread
    or fetch failure). Otherwise a few lines like "From: X | Subject: Y | snippet…"
    for up to max_messages preceding messages.
    """
    logs: list[dict] = []
    if not thread_id or thread_id == current_message_id:
        return "(none)", logs
    try:
        params = json.dumps({"userId": "me", "id": thread_id, "format": "metadata"})
        result = subprocess.run(  # nosec B603
            [gws, "gmail", "users", "threads", "get", "--params", params],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        logs.append({"level": "warn", "message": f"thread fetch timed out after {timeout}s"})
        return "(unavailable: timeout)", logs

    if result.returncode != 0:
        logs.append({"level": "warn", "message": f"thread fetch exited {result.returncode}"})
        return "(unavailable)", logs

    stdout = result.stdout.strip()
    brace = stdout.find("{")
    if brace > 0:
        stdout = stdout[brace:]
    try:
        thread_json = json.loads(stdout)
    except json.JSONDecodeError:
        logs.append({"level": "warn", "message": "thread fetch invalid JSON"})
        return "(unavailable: bad json)", logs

    messages = thread_json.get("messages") or []
    rows: list[str] = []
    for msg in messages:
        if not isinstance(msg, dict):
            continue
        if str(msg.get("id", "")) == current_message_id:
            continue
        headers = (msg.get("payload", {}) or {}).get("headers") or []
        sender = _header(headers, "From") or "(unknown)"
        subj = _header(headers, "Subject") or "(no subject)"
        snippet = str(msg.get("snippet", "")).strip()[:200]
        rows.append(f"- From: {sender} | Subject: {subj}\n  {snippet}")
    if not rows:
        return "(none)", logs
    rows = rows[-max_messages:]
    logs.append({"level": "debug", "message": f"thread context: {len(rows)} prior messages"})
    return "\n".join(rows), logs


def load_prompt_template(path: str) -> str:
    """Load and return the prompt template from disk. Raises on missing file or missing placeholders."""
    template_path = Path(os.path.expanduser(path))
    if not template_path.is_file():
        raise FileNotFoundError(f"prompt template not found: {template_path}")
    template = template_path.read_text(encoding="utf-8")
    missing = [p for p in REQUIRED_PLACEHOLDERS if "{" + p + "}" not in template]
    if missing:
        raise ValueError(f"prompt template at {template_path} missing required placeholders: {missing}")
    return template


def build_prompt(
    template: str,
    from_addr: str,
    subject: str,
    message_id: str,
    body: str,
    trust_level: str,
    pipeline_path: str,
    scores: dict[str, float],
    llm_score: float | None,
    attachments: str,
    mime_summary: str,
    thread_summary: str,
) -> str:
    score_summary = (
        f"regex={scores.get('regex', 0):.2f}, "
        f"pg2={scores.get('promptguard', 0):.2f}, "
        f"sentinel={scores.get('sentinel', 0):.2f}, "
        f"superagent={scores.get('classifier_a', 0):.2f}"
        + (f", llm={llm_score:.2f}" if llm_score is not None else "")
    )
    return template.format(
        from_addr=from_addr,
        subject=subject,
        message_id=message_id,
        trust_level=trust_level,
        pipeline_path=pipeline_path,
        score_summary=score_summary,
        body=body,
        attachments=attachments,
        mime_summary=mime_summary,
        thread_summary=thread_summary,
    )


def handle_email(req: dict[str, Any]) -> dict[str, Any]:
    config = req.get("config", {}) or {}
    event = req.get("event", {}) or {}
    payload = event.get("payload", {}) if isinstance(event, dict) else {}
    if not isinstance(payload, dict):
        payload = {}

    message_id = str(payload.get("message_id", "")).strip()
    if not message_id:
        return plugin_error("payload.message_id is required", retry=False)

    trust_level = str(payload.get("trust_level", "unknown"))
    pipeline_path = str(payload.get("path", "unknown"))
    scores = payload.get("scores", {})
    if not isinstance(scores, dict):
        scores = {}
    llm_score = payload.get("llm_score")
    if llm_score is not None:
        try:
            llm_score = float(llm_score)
        except (TypeError, ValueError):
            llm_score = None

    prompt_template_path = config.get("prompt_template_path")
    if not prompt_template_path:
        return plugin_error("config.prompt_template_path is required", retry=False)

    gws = str(config.get("gws_binary", DEFAULT_GWS))
    claude = str(config.get("claude_binary", DEFAULT_CLAUDE))
    cwd = str(config.get("claude_working_dir", DEFAULT_CLAUDE_CWD))
    timeout = int(config.get("timeout_seconds", DEFAULT_TIMEOUT))
    gws_timeout = int(config.get("gws_fetch_timeout_seconds", DEFAULT_GWS_FETCH_TIMEOUT))
    bot_address = str(config.get("bot_address", "") or "").strip().lower()
    thread_context_messages = int(
        config.get("thread_context_messages", DEFAULT_THREAD_CONTEXT_MESSAGES)
    )

    logs: list[dict] = [{"level": "info", "message": f"handling {message_id} trust={trust_level} path={pipeline_path}"}]

    try:
        template = load_prompt_template(str(prompt_template_path))
    except (FileNotFoundError, ValueError) as exc:
        return plugin_error(str(exc), retry=False, logs=logs)

    from_addr, subject, snippet, body, parts, thread_id, fetch_logs = fetch_message(
        gws, message_id, gws_timeout
    )
    logs.extend(fetch_logs)

    # Self-reply guard (belt-and-braces — triage should already drop these).
    if bot_address and from_addr:
        from_lower = from_addr.lower()
        if bot_address in from_lower:
            logs.append({
                "level": "info",
                "message": f"self-reply detected (from={from_addr!r}); skipping claude",
            })
            return plugin_ok(
                result=f"email {message_id} skipped (self-reply from {bot_address})",
                logs=logs,
            )

    mime_summary = build_mime_summary(parts)
    attachments = build_attachments_block(parts)
    thread_summary, thread_logs = fetch_thread_summary(
        gws, thread_id, message_id, thread_context_messages, gws_timeout
    )
    logs.extend(thread_logs)

    prompt = build_prompt(
        template, from_addr, subject, message_id, body,
        trust_level, pipeline_path, scores, llm_score,
        attachments, mime_summary, thread_summary,
    )

    try:
        result = subprocess.run(  # nosec B603
            [claude, "-p", "--dangerously-skip-permissions", prompt],
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=cwd,
        )
    except subprocess.TimeoutExpired:
        msg = f"claude timed out after {timeout}s"
        logs.append({"level": "error", "message": msg})
        return plugin_error(msg, retry=False, logs=logs)

    if result.returncode != 0:
        msg = f"claude exited {result.returncode}: {result.stderr[:200]}"
        logs.append({"level": "error", "message": msg})
        return plugin_error(msg, retry=False, logs=logs)

    output = result.stdout.strip()
    logs.append({"level": "info", "message": f"claude completed, output={len(output)} chars"})
    if output:
        logs.append({"level": "debug", "message": f"claude output: {output[:500]}"})

    return plugin_ok(result=f"email {message_id} handled at {iso_now()}", logs=logs)


def main() -> int:
    try:
        req = json.load(sys.stdin)
    except Exception as exc:  # nosec B110
        json.dump(plugin_error(f"invalid request json: {exc}", retry=False), sys.stdout)
        sys.stdout.write("\n")
        return 0

    if not isinstance(req, dict):
        json.dump(plugin_error("request must be a JSON object", retry=False), sys.stdout)
        sys.stdout.write("\n")
        return 0

    command = str(req.get("command", "")).strip()
    config = req.get("config", {})
    if not isinstance(config, dict):
        config = {}

    if command == "handle":
        resp = handle_email(req)
    elif command == "health":
        resp = handle_health(config)
    else:
        resp = plugin_error(f"unsupported command: {command!r}", retry=False)

    json.dump(resp, sys.stdout)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
