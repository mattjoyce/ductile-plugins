#!/usr/bin/env python3
"""jina-reader: Scrape web pages via Jina Reader API (r.jina.ai).

Protocol v2 plugin. Converts URLs to clean markdown via Jina's free
Reader API. Supports poll (configured URL) and handle (URL from event).

Config keys:
  url          - URL to scrape in poll mode (optional)
  max_size     - Max content bytes to keep (default: 102400 = 100KB)
  jina_api_key - Optional API key for higher rate limits
"""

from __future__ import annotations

import hashlib
import json
import sys
import urllib.error
import urllib.request
from typing import Any

DEFAULT_MAX_SIZE = 102_400


def ok_response(
    *,
    result: str,
    events: list[dict[str, Any]] | None = None,
    state_updates: dict[str, Any] | None = None,
    logs: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    resp: dict[str, Any] = {
        "status": "ok",
        "result": result,
        "logs": logs or [],
    }
    if events:
        resp["events"] = events
    if state_updates:
        resp["state_updates"] = state_updates
    return resp


def error_response(
    message: str,
    *,
    logs: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    return {
        "status": "error",
        "error": message,
        "retry": True,
        "logs": logs or [{"level": "error", "message": message}],
    }


def fetch_via_jina(url: str, *, max_size: int, api_key: str = "") -> tuple[str, bool]:
    """Fetch URL content as markdown via Jina Reader API."""
    jina_url = f"https://r.jina.ai/{url}"
    headers = {
        "Accept": "text/plain",
        "User-Agent": "ductile/jina-reader",
    }
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    req = urllib.request.Request(jina_url, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as resp:
        content = resp.read(max_size + 1)

    truncated = len(content) > max_size
    content = content[:max_size]
    return content.decode("utf-8", errors="replace"), truncated


def content_hash(text: str) -> str:
    """SHA-256 hash of content for change detection."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def handle_health() -> dict[str, Any]:
    return ok_response(
        result="healthy",
        logs=[{"level": "info", "message": "healthy"}],
    )


def handle_poll(config: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
    max_size = int(config.get("max_size", DEFAULT_MAX_SIZE))
    url = str(config.get("url") or "").strip()
    if not url:
        return error_response(
            "config.url required for poll command",
            logs=[{"level": "error", "message": "no url configured for poll"}],
        )

    try:
        markdown, truncated = fetch_via_jina(
            url,
            max_size=max_size,
            api_key=str(config.get("jina_api_key") or ""),
        )
    except (urllib.error.URLError, urllib.error.HTTPError, OSError) as exc:
        return error_response(
            f"fetch failed: {exc}",
            logs=[{"level": "error", "message": f"fetch failed for {url}: {exc}"}],
        )

    new_hash = content_hash(markdown)
    old_hash = str(state.get("content_hash") or "")
    changed = new_hash != old_hash

    events: list[dict[str, Any]] = []
    if changed:
        events.append(
            {
                "type": "content_changed",
                "payload": {
                    "url": url,
                    "content": markdown,
                    "content_hash": new_hash,
                    "truncated": truncated,
                },
            }
        )

    logs = [{"level": "info", "message": f"polled {url} (hash={new_hash}, changed={changed})"}]
    if truncated:
        logs.append({"level": "warn", "message": f"content truncated to {max_size} bytes"})

    return ok_response(
        result=f"polled {url} (changed={changed})",
        events=events,
        state_updates={"content_hash": new_hash, "last_url": url},
        logs=logs,
    )


def handle_handle(config: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
    max_size = int(config.get("max_size", DEFAULT_MAX_SIZE))
    payload = event.get("payload")
    if not isinstance(payload, dict):
        payload = {}
    url = str(payload.get("url") or event.get("url") or "").strip()
    if not url:
        return error_response(
            "event must include url",
            logs=[{"level": "error", "message": "handle: no url in event payload"}],
        )

    try:
        markdown, truncated = fetch_via_jina(
            url,
            max_size=max_size,
            api_key=str(config.get("jina_api_key") or ""),
        )
    except (urllib.error.URLError, urllib.error.HTTPError, OSError) as exc:
        return error_response(
            f"fetch failed: {exc}",
            logs=[{"level": "error", "message": f"fetch failed for {url}: {exc}"}],
        )

    logs = [{"level": "info", "message": f"scraped {url} ({len(markdown)} bytes)"}]
    if truncated:
        logs.append({"level": "warn", "message": f"content truncated to {max_size} bytes"})

    return ok_response(
        result=f"scraped {url} ({len(markdown)} bytes)",
        events=[
            {
                "type": "content_ready",
                "payload": {
                    "url": url,
                    "content": markdown,
                    "content_hash": content_hash(markdown),
                    "truncated": truncated,
                },
            }
        ],
        logs=logs,
    )


def handle_request(request: dict[str, Any]) -> dict[str, Any]:
    command = str(request.get("command") or "").strip()
    config = request.get("config")
    if not isinstance(config, dict):
        config = {}
    state = request.get("state")
    if not isinstance(state, dict):
        state = {}
    event = request.get("event")
    if not isinstance(event, dict):
        event = {}

    if command == "health":
        return handle_health()
    if command == "poll":
        return handle_poll(config, state)
    if command == "handle":
        return handle_handle(config, event)
    return error_response(
        f"unknown command: {command}",
        logs=[{"level": "error", "message": f"unknown command: {command}"}],
    )


def main() -> None:
    request = json.load(sys.stdin)
    response = handle_request(request)
    json.dump(response, sys.stdout)


if __name__ == "__main__":
    main()
