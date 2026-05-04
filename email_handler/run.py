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
DEFAULT_TIMEOUT = 300
DEFAULT_GWS_FETCH_TIMEOUT = 30
BODY_TRUNCATE_CHARS = 6000

REQUIRED_PLACEHOLDERS = (
    "from_addr", "subject", "message_id",
    "trust_level", "pipeline_path", "score_summary", "body",
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


def fetch_message(gws: str, message_id: str, timeout: int) -> tuple[str, str, str, str, list[dict]]:
    """Fetch full message via gws. Returns (from_addr, subject, snippet, body_text, logs)."""
    logs: list[dict] = []
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
            return "", "", "", f"[fetch failed: {result.stderr[:100]}]", logs

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
            return "", "", "", "[fetch failed: invalid JSON]", logs

        headers = msg_json.get("payload", {}).get("headers") or []
        from_addr = _header(headers, "From")
        subject = _header(headers, "Subject")
        snippet = str(msg_json.get("snippet", "")).strip()
        body_text = _extract_text(msg_json.get("payload", {}))
        if not body_text:
            body_text = snippet or "[no body text]"

        truncated = len(body_text) > BODY_TRUNCATE_CHARS
        body_text = body_text[:BODY_TRUNCATE_CHARS]
        logs.append({
            "level": "debug",
            "message": f"fetched message: from={from_addr!r} subject={subject!r}"
                       + (" [body truncated]" if truncated else ""),
        })
        return from_addr, subject, snippet, body_text, logs

    except subprocess.TimeoutExpired:
        logs.append({"level": "warn", "message": f"gws fetch timed out after {timeout}s"})
        return "", "", "", "[fetch timed out]", logs


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

    logs: list[dict] = [{"level": "info", "message": f"handling {message_id} trust={trust_level} path={pipeline_path}"}]

    try:
        template = load_prompt_template(str(prompt_template_path))
    except (FileNotFoundError, ValueError) as exc:
        return plugin_error(str(exc), retry=False, logs=logs)

    from_addr, subject, snippet, body, fetch_logs = fetch_message(gws, message_id, gws_timeout)
    logs.extend(fetch_logs)

    prompt = build_prompt(
        template, from_addr, subject, message_id, body,
        trust_level, pipeline_path, scores, llm_score,
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
