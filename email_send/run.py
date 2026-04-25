#!/usr/bin/env python3
"""email_send — ductile plugin (protocol v2).

Sends a single email via the `gws` CLI (Google Workspace command-line tool,
gmail.users.messages.send). Stateless: each invocation is independent, no
durable state observed, no fact_outputs declared.

Commands:
  send   : Send an email. Consumes to/subject/body_text/[body_html] from the
           event payload (falls back to config). Builds an RFC822 message,
           base64url-encodes it, posts via `gws gmail users messages send`.
           Emits email_send.sent on success.
  health : Verify gws binary + gmail auth via gmail.users.getProfile.
           Emits no state_updates.

Config keys (all optional):
  gws_binary               path/name of gws executable (default: "gws")
  default_to               fallback recipient if event payload doesn't supply one
  send_timeout_seconds     subprocess timeout for send (default: 30)
  health_timeout_seconds   subprocess timeout for getProfile (default: 15)

Event payload schema (consumed by `send`):
  to            string   - recipient email (required if no default_to)
  subject       string   - subject line (required)
  body_text     string   - plain-text body (required)
  body_html     string   - optional HTML body; if present, sends multipart/alternative
  dry_run       bool     - if true, append --dry-run to gws (validates locally, no API send)
"""

import base64
import json
import subprocess
import sys
from datetime import datetime, timezone
from email.message import EmailMessage
from typing import Any, Dict, Optional

DEFAULT_GWS = "gws"
DEFAULT_SEND_TIMEOUT = 30
DEFAULT_HEALTH_TIMEOUT = 15


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def log(level: str, message: str) -> Dict[str, str]:
    return {"level": level, "message": message}


def ok_response(
    *,
    result: str,
    events: Optional[list] = None,
    logs: Optional[list] = None,
) -> Dict[str, Any]:
    response: Dict[str, Any] = {"status": "ok", "result": result}
    if events:
        response["events"] = events
    response["logs"] = logs or []
    return response


def error_response(message: str, *, retry: bool = True) -> Dict[str, Any]:
    return {
        "status": "error",
        "error": message,
        "retry": retry,
        "logs": [log("error", message)],
    }


class GWSError(Exception):
    def __init__(self, message: str, *, retry: bool = False):
        super().__init__(message)
        self.retry = retry


def gws_run(binary: str, *args: str, timeout: int = DEFAULT_SEND_TIMEOUT) -> Dict[str, Any]:
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

    stdout = result.stdout.strip()
    if not stdout:
        raise GWSError(
            f"gws returned empty output (exit {result.returncode}, stderr={result.stderr.strip()[:200]!r})",
            retry=True,
        )

    try:
        data = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise GWSError(f"gws output is not valid JSON: {exc}", retry=False)

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


def extract_event_payload(request: Dict[str, Any]) -> Dict[str, Any]:
    event = request.get("event")
    if isinstance(event, dict):
        payload = event.get("payload")
        if isinstance(payload, dict):
            return payload
    config = request.get("config")
    if isinstance(config, dict):
        return config
    return {}


def build_rfc822(*, to: str, subject: str, body_text: str, body_html: Optional[str]) -> str:
    """Build an RFC822 message and return base64url-encoded raw string for gws."""
    msg = EmailMessage()
    msg["To"] = to
    msg["Subject"] = subject
    msg.set_content(body_text)
    if body_html:
        msg.add_alternative(body_html, subtype="html")

    raw_bytes = msg.as_bytes()
    encoded = base64.urlsafe_b64encode(raw_bytes).decode("ascii").rstrip("=")
    return encoded


def send_command(request: Dict[str, Any]) -> Dict[str, Any]:
    config = request.get("config") or {}
    binary = str(config.get("gws_binary") or DEFAULT_GWS)
    timeout = int(config.get("send_timeout_seconds") or DEFAULT_SEND_TIMEOUT)
    default_to = str(config.get("default_to") or "").strip()
    default_dry_run = bool(config.get("default_dry_run") or False)

    payload = extract_event_payload(request)
    to = str(payload.get("to") or default_to).strip()
    subject = str(payload.get("subject") or "").strip()
    body_text = payload.get("body_text")
    body_html = payload.get("body_html")
    dry_run = bool(payload.get("dry_run", default_dry_run))

    if not to:
        return error_response("send requires payload.to (or config.default_to)", retry=False)
    if not subject:
        return error_response("send requires payload.subject", retry=False)
    if not body_text or not isinstance(body_text, str):
        return error_response("send requires payload.body_text (string)", retry=False)
    if body_html is not None and not isinstance(body_html, str):
        return error_response("payload.body_html must be a string when provided", retry=False)

    try:
        raw_b64url = build_rfc822(
            to=to,
            subject=subject,
            body_text=body_text,
            body_html=body_html,
        )
    except Exception as exc:  # noqa: BLE001
        return error_response(f"failed to build RFC822 message: {exc}", retry=False)

    body_json = json.dumps({"raw": raw_b64url})

    gws_args = ["gmail", "users", "messages", "send",
                "--params", '{"userId":"me"}',
                "--json", body_json]
    if dry_run:
        gws_args.append("--dry-run")

    try:
        data = gws_run(binary, *gws_args, timeout=timeout)
    except GWSError as exc:
        return error_response(f"gws send failed: {exc}", retry=exc.retry)

    message_id = data.get("id")
    thread_id = data.get("threadId")
    sent_at = now_iso()

    out_payload = {
        "to": to,
        "subject": subject,
        "message_id": message_id,
        "thread_id": thread_id,
        "sent_at": sent_at,
        "dry_run": dry_run,
    }

    result_msg = f"DRY-RUN validated send to {to}" if dry_run else f"sent to {to} (id={message_id})"

    return ok_response(
        result=result_msg,
        events=[{"type": "email_send.sent", "payload": out_payload}],
        logs=[log("info", f"{'dry-run' if dry_run else 'sent'} message_id={message_id} to={to}")],
    )


def health_command(request: Dict[str, Any]) -> Dict[str, Any]:
    config = request.get("config") or {}
    binary = str(config.get("gws_binary") or DEFAULT_GWS)
    timeout = int(config.get("health_timeout_seconds") or DEFAULT_HEALTH_TIMEOUT)

    try:
        data = gws_run(
            binary,
            "gmail", "users", "getProfile",
            "--params", '{"userId":"me"}',
            timeout=timeout,
        )
    except GWSError as exc:
        return error_response(f"gws health check failed: {exc}", retry=exc.retry)

    email_address = data.get("emailAddress")
    payload = {
        "healthy": True,
        "email_address": email_address,
        "checked_at": now_iso(),
    }

    return ok_response(
        result=f"healthy: gmail auth ok for {email_address}",
        events=[{"type": "email_send.health", "payload": payload}],
        logs=[log("info", f"gmail auth verified for {email_address}")],
    )


def main() -> None:
    try:
        request = json.load(sys.stdin)
    except Exception as exc:  # noqa: BLE001
        json.dump(error_response(f"Invalid request JSON: {exc}", retry=False), sys.stdout)
        return

    command = str(request.get("command", "")).strip().lower()

    if command == "send":
        response = send_command(request)
    elif command == "health":
        response = health_command(request)
    else:
        response = error_response(f"Unknown command: {command}", retry=False)

    json.dump(response, sys.stdout)


if __name__ == "__main__":
    main()
